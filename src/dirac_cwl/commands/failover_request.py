"""LHCb command for committing the status of the files in the file report.

The status will be "Processed" if everything ended properly or "Unused" if it did not.
"""

import logging
import os

from DIRAC.Core.Utilities.ReturnValues import SErrorException, returnValueOrRaise
from LHCbDIRAC.Workflow.Modules.FailoverRequest import _prepareRequest

from .core import PostProcessCommand
from .workflow_commons import StepStatus, WorkflowCommons

logger = logging.getLogger(__name__)


class FailoverRequest(PostProcessCommand):
    """Commits the status of the files in the file report.

    The status will be "Processed" if everything ended properly or "Unused" if it did not.
    """

    def _execute(self, job_path: os.PathLike, workflow_commons: WorkflowCommons, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param workflow_commons: WorkflowCommons object.
        :param kwargs: Additional keyword arguments.
        """
        _prepareRequest(workflow_commons.request, workflow_commons.job_id)

        filesInFileReport = workflow_commons.file_report.getFiles()

        for lfn in workflow_commons.inputs:
            if lfn not in filesInFileReport:
                status = "Processed" if workflow_commons.step_status == StepStatus.Done else "Unused"
                if status == "Unused":
                    logger.info("Set status of %s to 'Unused' due to workflow failure", lfn)
                else:
                    logger.debug("No status populated for %s, setting to 'Processed'", lfn)

                workflow_commons.file_report.setFileStatus(int(workflow_commons.production_id), lfn, status)

        try:
            value = returnValueOrRaise(workflow_commons.file_report.commit())
            if value:
                logger.info("Status of files have been properly updated in the TransformationDB")
            else:
                logger.warning("No file status update reported. There are no input files?")
        except SErrorException as e:
            logger.error("Something went wrong trying fileReport.commit() %s", e)

        if workflow_commons.file_report.getFiles():
            logger.error("On first attempt, failed to report file status to TransformationDB")
            try:
                value = returnValueOrRaise(workflow_commons.file_report.generateForwardDISET())
                if not value:
                    logger.info("On second attempt, files correctly reported to TransformationDB")
                elif workflow_commons.step_status == StepStatus.Done:
                    logger.info("Adding a SetFileStatus operation to the request")
                    workflow_commons.request.addOperation(value)
                else:
                    logger.info("The job should fail: do not set requests, as the DRA will take care")
            except SErrorException as e:
                logger.warning("Could not generate Operation for file report: %s", e)

        if workflow_commons.step_status == StepStatus.Done:
            workflow_commons.job_report.setApplicationStatus("Job Finished Successfully", True)

        workflow_commons.generateFailoverFile()
