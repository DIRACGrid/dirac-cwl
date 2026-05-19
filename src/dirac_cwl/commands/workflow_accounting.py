"""LHCb command for preparing and sending accounting information to the DIRAC Accounting system.

Formerly known as StepAccounting.
"""

import datetime
import logging
import os
from typing import Any, Dict

from DIRAC import gConfig
from DIRAC.Core.Utilities.ReturnValues import SErrorException, returnValueOrRaise
from DIRAC.Workflow.Utilities.Utils import getStepCPUTimes
from LHCbDIRAC.AccountingSystem.Client.Types.JobStep import JobStep

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .core import PostProcessCommand
from .workflow_commons import WorkflowCommons

logger = logging.getLogger(__name__)


class WorkflowAccounting(PostProcessCommand):
    """Prepares and sends accounting information to the DIRAC Accounting system."""

    def _execute(self, job_path: os.PathLike, workflow_commons: WorkflowCommons, **kwargs) -> None:
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param workflow_commons: WorkflowCommons object.
        :param kwargs: Additional keyword arguments.
        """
        # Obtain Workflow Commons
        cpu_times: Dict[str, Any] = {}
        if workflow_commons.start_time:
            cpu_times["StartTime"] = workflow_commons.start_time
        if workflow_commons.start_stats:
            cpu_times["StartStats"] = workflow_commons.start_stats

        exec_time, cpu_time = getStepCPUTimes(cpu_times)

        cpuNormFactor = gConfig.getValue("/LocalSite/CPUNormalizationFactor", 0.0)
        normCPU = cpu_time * cpuNormFactor

        job_step = JobStep()

        if not workflow_commons.xf_o:
            logger.error("XML Summary object could not be found (not produced?), skipping the report")
            return

        now = datetime.datetime.now(datetime.UTC)
        job_step.setStartTime(now)
        job_step.setEndTime(now)

        dataDict = {
            "JobGroup": str(workflow_commons.production_id),
            "RunNumber": workflow_commons.run_number,
            "EventType": workflow_commons.event_type,
            "ProcessingType": workflow_commons.step_proc_pass,  # this is the processing pass of the step
            "ProcessingStep": workflow_commons.bk_step_id,  # the step ID
            "Site": workflow_commons.site_name,
            "FinalStepState": workflow_commons.step_status,
            "CPUTime": cpu_time,
            "NormCPUTime": normCPU,
            "ExecTime": exec_time * workflow_commons.number_of_processors,
            "InputData": sum(workflow_commons.xf_o.inputFileStats.values()),
            "OutputData": sum(workflow_commons.xf_o.outputFileStats.values()),
            "InputEvents": workflow_commons.xf_o.inputEventsTotal,
            "OutputEvents": workflow_commons.xf_o.outputEventsTotal,
        }

        job_step.setValuesFromDict(dataDict)

        try:
            returnValueOrRaise(job_step.checkValues())
        except SErrorException as e:
            logger.error("Values for StepAccounting are wrong: Here are the given data: %s", dataDict, exc_info=e)
            raise WorkflowProcessingException(
                f"Values for StepAccounting are wrong. Here are the given data: {dataDict}"
            ) from e

        workflow_commons.dsc.addRegister(job_step)
