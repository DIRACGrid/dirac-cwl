"""Post-processing command for uploading logging information to a Storage Element."""

import logging
import os
import shlex

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.ReturnValues import SErrorException, returnSingleResult, returnValueOrRaise
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.Resources.Storage.StorageElement import StorageElement
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.Core.Utilities.ProductionData import getLogPath
from LHCbDIRAC.Workflow.Modules.FailoverRequest import _prepareRequest
from LHCbDIRAC.Workflow.Modules.UploadLogFile import (
    _createLogUploadRequest,
    _determineRelevantFiles,
    _get_log_url,
    _populateLogDirectory,
    _setLogFilePermissions,
    _uploadLogToFailoverSE,
    _zip_files,
)

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .core import PostProcessCommand
from .workflow_commons import StepStatus, WorkflowCommons

logger = logging.getLogger(__name__)


class UploadLogFile(PostProcessCommand):
    """Post-processing command for log file uploading."""

    def _execute(self, job_path: os.PathLike[str], workflow_commons: WorkflowCommons, **kwargs):
        """Execute the log uploading process.

        :param job_path: Path to the job working directory.
        :param workflow_commons: WorkflowCommons object.
        :param kwargs: Additional keyword arguments.
        """
        if workflow_commons.step_status == StepStatus.Failed:
            return

        if not self.request:
            self.request = Request(workflow_commons.request_dict)
        if not self.failover_transfer:
            self.failover_transfer = FailoverTransfer(self.request)

        log_lfn_path = workflow_commons.log_target_path
        if not log_lfn_path:
            parameters = {
                "PRODUCTION_ID": workflow_commons.production_id,
                "JOB_ID": workflow_commons.job_id,
                "configName": workflow_commons.config_name,
                "configVersion": workflow_commons.config_version,
            }
            try:
                log_dict = returnValueOrRaise(getLogPath(parameters, self.bk_client))
            except SErrorException as e:
                raise WorkflowProcessingException("Could not create LogFilePath") from e
            log_lfn_path = log_dict["LogTargetPath"][0]

        if not isinstance(log_lfn_path, str):
            log_lfn_path = log_lfn_path[0]

        workflow_commons.log_lfn_path = log_lfn_path

        ops = Operations()
        log_se = ops.getValue("LogStorage/LogSE", "LogSE")
        log_extensions = ops.getValue("LogFiles/Extensions", [])

        _prepareRequest(self.request, workflow_commons.job_id)

        try:
            file_list = returnValueOrRaise(systemCall(0, shlex.split("ls -al")))
        except SErrorException as e:
            logger.error("Failed to list the log directory\n%s", e)

        if file_list:
            logger.info("The contents of the working directory...")
            logger.info(str(file_list[1]))
        else:
            logger.error("Failed to list the log directory\n%s", str(file_list[2]))

        workflow_commons.log_dir = os.path.realpath(
            os.path.join(job_path, f"./job/log/{workflow_commons.production_id}/{workflow_commons.prod_job_id}")
        )
        logger.info("Selected log files will be temporarily stored in %s", workflow_commons.log_dir)

        ##########################################
        # First determine the files which should be saved
        logger.info("Determining the files to be saved in the logs.")

        try:
            selected_files = returnValueOrRaise(_determineRelevantFiles(log_extensions))
        except SErrorException as e:
            logger.error("Completely failed to select relevant log files.", exc_info=e)
            return  # Does not fail

        logger.info("The following files were selected to be saved\n%s", selected_files)

        #########################################
        # Create a temporary directory containing these files
        logger.info("Determining the files to be saved in the logs.")

        try:
            returnValueOrRaise(_populateLogDirectory(selected_files, workflow_commons.log_dir))
        except SErrorException as e:
            logger.error("Completely failed to populate temporary log file directory.", stack_info=e)
            self.job_report.setApplicationStatus("Failed To Populate Log Dir")
            return  # Does not fail

        logger.debug("%s populated with log files.", workflow_commons.log_dir)

        #########################################
        # Make sure all the files in the log directory have the correct permissions
        try:
            returnValueOrRaise(_setLogFilePermissions(workflow_commons.log_dir))
        except SErrorException as e:
            logger.error("Could not set permissions of log files to 0755 with message:\n%s", e)

        # zip all files
        try:
            zip_file_name = returnValueOrRaise(_zip_files(workflow_commons.prod_job_id, selected_files))
        except SErrorException as e:
            logger.error("Failed to create zip of log files %s", e)
            self.job_report.setApplicationStatus("Failed to create zip of log files")
            return  # Does not fail

        logger.info("Transferring zipped log files to the %s", log_se)

        # logFilePath is something like /lhcb/MC/2016/LOG/00095376/0000/
        # the zipFileName should have the same name, e.g. 00000381.zip
        zip_path = os.path.join(workflow_commons.log_file_path, zip_file_name)
        log_https_url = _get_log_url(log_se, zip_path)

        logger.info("putFile %s to %s", zip_file_name, log_se)

        try:
            returnValueOrRaise(returnSingleResult(StorageElement(log_se).putFile({zip_path: zip_file_name})))
            logger.info("Successfully upload log file to %s", log_se)
            logger.info("Logs for this job may be retrieved from %s", log_https_url)

        except SErrorException as e:
            logger.error("Failed to upload log files with message %s", e)
            logger.error("Now uploading to failover SE")

            try:
                upload_result_dict = returnValueOrRaise(
                    _uploadLogToFailoverSE(
                        self.failover_transfer,
                        zip_file_name,
                        log_lfn_path,
                        workflow_commons.site_name,
                    )
                )

                uploaded_se = upload_result_dict["uploadedSE"]

                logger.info("Uploading logs to failover SE '%s'", uploaded_se)
                logger.info("Setting log upload request for %s at %s", log_lfn_path, log_se)

                _createLogUploadRequest(self.failover_transfer.request, log_se, log_lfn_path, uploaded_se)

                logger.debug("Successfully created failover request")

            except SErrorException as e:
                logger.error(
                    "Failed to upload logs to all failover destinations (the job will not fail for this reason"
                )
                self.job_report.setApplicationStatus("Failed To Upload Logs")

        # While it's the zip file that is uploaded, we set in job parameters its directory,
        # as the .zip is deflated automatically
        self.job_report.setJobParameter(
            "Log URL", f"<a href=\"{log_https_url.replace('.zip','/')}\">Log file directory</a>"
        )

    def _resolve_clients(self, workflow_commons: WorkflowCommons):
        super()._resolve_clients(workflow_commons)

        if not self.bk_client:
            self.bk_client = BookkeepingClient()
