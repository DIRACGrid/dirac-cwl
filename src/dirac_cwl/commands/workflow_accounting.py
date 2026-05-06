"""LHCb command for preparing and sending accounting information to the DIRAC Accounting system.

Formerly known as StepAccounting.
"""

import os
from datetime import datetime

from DIRAC import gConfig
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.Workflow.Utilities.Utils import getStepCPUTimes
from LHCbDIRAC.AccountingSystem.Client.Types.JobStep import JobStep
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary
from LHCbDIRAC.Workflow.Modules.BookkeepingReport import _generate_xml_object

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .core import PostProcessCommand
from .utils import prepare_lhcb_workflow_commons, save_workflow_commons


class WorkflowAccounting(PostProcessCommand):
    """Prepares and sends accounting information to the DIRAC Accounting system."""

    def execute(self, job_path, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        failed = False
        try:
            # Obtain Workflow Commons
            workflow_commons_path = kwargs.get("workflow_commons_path", os.path.join(job_path, "workflow_commons.json"))
            workflow_commons = {}
            workflow_commons = prepare_lhcb_workflow_commons(
                workflow_commons_path,
                extra_mandatory_values=["bk_step_id", "step_proc_pass", "event_type"],
                extra_default_values={
                    "step_proc_pass": "",
                    "run_number": "Unknown",
                },
            )

            cpu_times = {}
            if "start_time" in workflow_commons:
                cpu_times["StartTime"] = workflow_commons["start_time"]
            if "start_stats" in workflow_commons:
                cpu_times["StartStats"] = workflow_commons["start_stats"]

            exec_time, cpu_time = getStepCPUTimes(cpu_times)

            cpuNormFactor = gConfig.getValue("/LocalSite/CPUNormalizationFactor", 0.0)
            normCPU = cpu_time * cpuNormFactor

            jobStep = JobStep()

            if "xml_summary_path" in workflow_commons:
                xf_o = XMLSummary(workflow_commons["xml_summary_path"])
            else:
                xf_o = _generate_xml_object(
                    workflow_commons["cleaned_application_name"],
                    workflow_commons["production_id"],
                    workflow_commons["prod_job_id"],
                    workflow_commons["command_number"],
                    workflow_commons["command_id"],
                )

            now = datetime.utcnow()
            jobStep.setStartTime(now)
            jobStep.setEndTime(now)

            dataDict = {
                "JobGroup": str(workflow_commons["production_id"]),
                "RunNumber": workflow_commons["run_number"],
                "EventType": workflow_commons["event_type"],
                "ProcessingType": workflow_commons["step_proc_pass"],  # this is the processing pass of the step
                "ProcessingStep": workflow_commons["bk_step_id"],  # the step ID
                "Site": workflow_commons["site_name"],
                "FinalStepState": workflow_commons["step_status"],
                "CPUTime": cpu_time,
                "NormCPUTime": normCPU,
                "ExecTime": exec_time * workflow_commons["number_of_processors"],
                "InputData": sum(xf_o.inputFileStats.values()),
                "OutputData": sum(xf_o.outputFileStats.values()),
                "InputEvents": xf_o.inputEventsTotal,
                "OutputEvents": xf_o.outputEventsTotal,
            }

            jobStep.setValuesFromDict(dataDict)

            res = jobStep.checkValues()
            if not res["OK"]:
                raise WorkflowProcessingException(
                    "Values for StepAccounting are wrong:", f"{res['Message']}. Here are the given data: {dataDict}"
                )

            dsc = DataStoreClient()
            dsc.addRegister(jobStep)
            workflow_commons["accounting_registers"] = dsc.__registersList

        except Exception as e:
            failed = True
            raise WorkflowProcessingException() from e

        finally:
            if workflow_commons:
                save_workflow_commons(workflow_commons, workflow_commons_path, failed=failed)
