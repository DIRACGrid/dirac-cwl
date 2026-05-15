"""LHCb command for checking the XMLSummary output to ensure that the execution was done correctly."""

import os

from LHCbDIRAC.Workflow.Modules.AnalyseXMLSummary import _areInputsOK, _isXMLSummaryOK
from LHCbDIRAC.Workflow.Modules.BookkeepingReport import _generate_xml_object

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .core import PostProcessCommand
from .workflow_commons import StepStatus, WorkflowCommons


class AnalyseXmlSummary(PostProcessCommand):
    """Performs a series of checks on the XMLSummary output to make sure the execution was done correctly."""

    def execute(self, job_path: os.PathLike, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        failed = False
        workflow_commons = None
        try:
            workflow_commons = WorkflowCommons.load(job_path)

            if workflow_commons.step_status == StepStatus.Failed:
                return

            if not workflow_commons.xf_o:
                workflow_commons.xf_o = _generate_xml_object(
                    workflow_commons.cleaned_application_name,
                    workflow_commons.production_id,
                    workflow_commons.prod_job_id,
                    workflow_commons.step_number,
                    workflow_commons.step_id,
                )

            jobOk = _isXMLSummaryOK(workflow_commons.xf_o)

            if jobOk:
                jobOk = _areInputsOK(
                    workflow_commons.xf_o,
                    workflow_commons.inputs,
                    workflow_commons.number_of_events,
                    workflow_commons.production_id,
                    workflow_commons.file_report,
                )
            if not jobOk:
                workflow_commons.job_report.setApplicationStatus("XMLSummary reports error")
                raise WorkflowProcessingException("XMLSummary reports error")

            workflow_commons.job_report.setApplicationStatus(f"{workflow_commons.application_name} Step OK")

        except Exception as e:
            failed = True
            raise WorkflowProcessingException(e) from e

        finally:
            if workflow_commons:
                workflow_commons.save(job_path, failed=failed)
