"""LHCb command for checking the XMLSummary output to ensure that the execution was done correctly."""

import os

from DIRAC.TransformationSystem.Client.FileReport import FileReport
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary
from LHCbDIRAC.Workflow.Modules.AnalyseXMLSummary import _areInputsOK, _isXMLSummaryOK
from LHCbDIRAC.Workflow.Modules.BookkeepingReport import _generate_xml_object

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .core import PostProcessCommand
from .utils import prepare_lhcb_workflow_commons, save_workflow_commons


class AnalyseXmlSummary(PostProcessCommand):
    """Performs a series of checks on the XMLSummary output to make sure the execution was done correctly."""

    def execute(self, job_path, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        failed = False
        try:
            workflow_commons_path = kwargs.get("workflow_commons_path", os.path.join(job_path, "workflow_commons.json"))

            workflow_commons = prepare_lhcb_workflow_commons(
                workflow_commons_path,
                extra_mandatory_values=[
                    "bk_step_id",
                ],
                extra_default_values={
                    "bookkeeping_LFNs": [],
                    "size": {},
                    "md5": {},
                    "guid": {},
                    "sim_description": "NoSimConditions",
                },
            )

            if not workflow_commons["step_status"]["OK"]:
                return

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

            file_report = FileReport()
            job_report = JobReport(workflow_commons["job_id"])

            file_report.statusDict = workflow_commons["file_report_files_dict"]

            jobOk = _isXMLSummaryOK(xf_o)

            if jobOk:
                jobOk = _areInputsOK(
                    xf_o,
                    workflow_commons["inputs"],
                    workflow_commons["number_of_events"],
                    workflow_commons["production_id"],
                    file_report,
                )
            if not jobOk:
                job_report.setApplicationStatus("XMLSummary reports error")
                raise WorkflowProcessingException("XMLSummary reports error")

            job_report.setApplicationStatus(f"{workflow_commons['application_name']} Step OK")

        except:
            failed = True
            raise

        finally:
            save_workflow_commons(workflow_commons, workflow_commons_path, failed=failed)
