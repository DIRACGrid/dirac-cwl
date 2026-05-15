"""LHCb command for preparing and sending accounting information to the DIRAC Accounting system.

Formerly known as StepAccounting.
"""

import datetime
import os

from DIRAC import gConfig
from DIRAC.Workflow.Utilities.Utils import getStepCPUTimes
from LHCbDIRAC.AccountingSystem.Client.Types.JobStep import JobStep

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .core import PostProcessCommand
from .workflow_commons import WorkflowCommons


class WorkflowAccounting(PostProcessCommand):
    """Prepares and sends accounting information to the DIRAC Accounting system."""

    def execute(self, job_path: os.PathLike, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        failed = False
        workflow_commons = None
        try:
            # Obtain Workflow Commons
            workflow_commons = WorkflowCommons.load(job_path)

            cpu_times = {}
            if "start_time" in workflow_commons:
                cpu_times["StartTime"] = workflow_commons.start_time
            if "start_stats" in workflow_commons:
                cpu_times["StartStats"] = workflow_commons.start_stats

            exec_time, cpu_time = getStepCPUTimes(cpu_times)

            cpuNormFactor = gConfig.getValue("/LocalSite/CPUNormalizationFactor", 0.0)
            normCPU = cpu_time * cpuNormFactor

            job_step = JobStep()

            if not workflow_commons.xf_o:
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

            res = job_step.checkValues()
            if not res["OK"]:
                raise WorkflowProcessingException(
                    "Values for StepAccounting are wrong:", f"{res['Message']}. Here are the given data: {dataDict}"
                )

            workflow_commons.accounting_registers.append(list(job_step.getValues()))

        except Exception as e:
            failed = True
            raise WorkflowProcessingException(e) from e

        finally:
            if workflow_commons:
                workflow_commons.save(job_path, failed=failed)
