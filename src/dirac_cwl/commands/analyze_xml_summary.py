"""LHCb command for checking the XMLSummary output to ensure that the execution was done correctly."""

import logging
import os

from LHCbDIRAC.Workflow.Modules.AnalyseXMLSummary import _areInputsOK, _isXMLSummaryOK

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .core import PostProcessCommand
from .workflow_commons import Step, StepStatus, WorkflowCommons

logger = logging.getLogger(__name__)


class AnalyseXmlSummary(PostProcessCommand):
    """Performs a series of checks on the XMLSummary output to make sure the execution was done correctly."""

    def _execute(self, job_path: os.PathLike, workflow_commons: WorkflowCommons, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param workflow_commons: WorkflowCommons object
        :param kwargs: Additional keyword arguments.
        """
        for step in workflow_commons.steps:
            self._execute_for_step(job_path, workflow_commons, step, **kwargs)

    def _execute_for_step(self, job_path: os.PathLike, workflow_commons: WorkflowCommons, step_commons: Step, **kwargs):
        """Execute the command for a specific step."""
        jobOk = _isXMLSummaryOK(step_commons.xf_o)

        if jobOk:
            jobOk = _areInputsOK(
                step_commons.xf_o,
                step_commons.inputs,
                step_commons.number_of_events,
                workflow_commons.production_id,
                workflow_commons.file_report,
            )
        if not jobOk:
            workflow_commons.job_report.setApplicationStatus("XMLSummary reports error")
            raise WorkflowProcessingException("XMLSummary reports error")

        if workflow_commons.step_status == StepStatus.Failed:
            logger.info("Workflow already failed")
            return

        workflow_commons.job_report.setApplicationStatus(f"{step_commons.application_name} Step OK")
