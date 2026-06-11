"""LHCb command for preparing and sending accounting information to the DIRAC Accounting system.

Formerly known as StepAccounting.
"""

import datetime
import logging
import os
from typing import Any, Dict

from DIRAC import gConfig
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.Core.Utilities.ReturnValues import SErrorException, returnValueOrRaise
from DIRAC.Workflow.Utilities.Utils import getStepCPUTimes
from LHCbDIRAC.AccountingSystem.Client.Types.JobStep import JobStep

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .core import PostProcessCommand
from .workflow_commons import Step, WorkflowCommons

logger = logging.getLogger(__name__)


class RegisterAccountingReport(PostProcessCommand):
    """Prepares and sends accounting information to the DIRAC Accounting system."""

    def _execute(self, job_path: os.PathLike[str], workflow_commons: WorkflowCommons, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param workflow_commons: WorkflowCommons object
        :param kwargs: Additional keyword arguments.
        """
        for step in workflow_commons.steps:
            self._execute_for_step(job_path, workflow_commons, step, **kwargs)

    def _execute_for_step(
        self, job_path: os.PathLike[str], workflow_commons: WorkflowCommons, step_commons: Step, **kwargs
    ):
        cpu_times: Dict[str, Any] = {}
        if step_commons.start_time:
            cpu_times["StartTime"] = step_commons.start_time
        if step_commons.start_stats:
            cpu_times["StartStats"] = step_commons.start_stats

        if not step_commons.application_name:
            logger.info("Not an application step: it will not be accounted")
            return

        exec_time, cpu_time = getStepCPUTimes(cpu_times)

        cpu_power = gConfig.getValue("/LocalSite/CPUNormalizationFactor", 1.0)
        norm_cpu = cpu_time * cpu_power

        job_step = JobStep()

        xf_o = step_commons.xf_o

        if not xf_o:
            logger.error("XML Summary object could not be found (not produced?), skipping the report")
            return

        now = datetime.datetime.now(datetime.UTC)
        job_step.setStartTime(now)
        job_step.setEndTime(now)

        data_dict = {
            "JobGroup": str(workflow_commons.production_id),
            "RunNumber": workflow_commons.run_number,
            "EventType": step_commons.event_type,
            "ProcessingType": step_commons.proc_pass,  # this is the processing pass of the step
            "ProcessingStep": step_commons.bk_id,  # the step ID
            "Site": workflow_commons.site_name,
            "FinalStepState": workflow_commons.step_status,
            "CPUTime": cpu_time,
            "NormCPUTime": norm_cpu,
            "ExecTime": exec_time * workflow_commons.number_of_processors,
            "InputData": sum(xf_o.inputFileStats.values()),
            "OutputData": sum(xf_o.outputFileStats.values()),
            "InputEvents": xf_o.inputEventsTotal,
            "OutputEvents": xf_o.outputEventsTotal,
        }

        job_step.setValuesFromDict(data_dict)

        try:
            returnValueOrRaise(job_step.checkValues())
        except SErrorException as e:
            logger.error("Values for StepAccounting are wrong: Here are the given data: %s", data_dict, exc_info=e)
            raise WorkflowProcessingException(
                f"Values for StepAccounting are wrong. Here are the given data: {data_dict}"
            ) from e

        self.dsc.addRegister(job_step)

    def _resolve_clients(self, workflow_commons: WorkflowCommons):
        super()._resolve_clients(workflow_commons)

        if not self.dsc:
            self.dsc = DataStoreClient()
