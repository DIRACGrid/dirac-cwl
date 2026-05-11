"""Post-processing command for uploading logging information to a Storage Element."""

import os
import shlex

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
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
from .utils import prepare_lhcb_workflow_commons, save_workflow_commons


class UploadLogFile(PostProcessCommand):
    """Post-processing command for log file uploading."""

    def execute(self, job_path, **kwargs):
        """Execute the log uploading process.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        # Obtain workflow information
        failed = False
        workflow_commons = {}
        request = None
        try:
            workflow_commons_path = kwargs.get("workflow_commons_path", os.path.join(job_path, "workflow_commons.json"))

            workflow_commons = prepare_lhcb_workflow_commons(
                workflow_commons_path,
                extra_mandatory_values=[],
                extra_default_values={"log_target_path": None, "log_file_path": ""},
            )
            request = Request(workflow_commons["request_dict"])

            if not workflow_commons["step_status"]["OK"]:
                return

            log_lfn_path = workflow_commons["log_target_path"]
            if not log_lfn_path:
                parameters = {
                    "PRODUCTION_ID": workflow_commons["production_id"],
                    "JOB_ID": workflow_commons["job_id"],
                    "configName": workflow_commons["config_name"],
                    "configVersion": workflow_commons["config_version"],
                }
                result = getLogPath(parameters, BookkeepingClient())
                if not result["OK"]:
                    raise WorkflowProcessingException("Could not create LogFilePath", result["Message"])
                log_lfn_path = result["Value"]["LogTargetPath"][0]

            if not isinstance(log_lfn_path, str):
                log_lfn_path = log_lfn_path[0]

            workflow_commons["log_lfn_path"] = log_lfn_path

            ops = Operations()
            log_se = ops.getValue("LogStorage/LogSE", "LogSE")
            log_extensions = ops.getValue("LogFiles/Extensions", [])

            _prepareRequest(request, workflow_commons["job_id"])
            failover_transfer = FailoverTransfer(request)
            job_report = JobReport(workflow_commons["job_id"])

            res = systemCall(0, shlex.split("ls -al"))

            workflow_commons["log_dir"] = os.path.realpath(
                os.path.join(
                    job_path, f"./job/log/{workflow_commons['production_id']}/{workflow_commons['prod_job_id']}"
                )
            )

            ##########################################
            # First determine the files which should be saved
            res = _determineRelevantFiles(log_extensions)
            if not res["OK"]:
                return
            selectedFiles = res["Value"]

            #########################################
            # Create a temporary directory containing these files
            res = _populateLogDirectory(selectedFiles, workflow_commons["log_dir"])
            if not res["OK"]:
                job_report.setApplicationStatus("Failed To Populate Log Dir")
                return

            #########################################
            # Make sure all the files in the log directory have the correct permissions
            result = _setLogFilePermissions(workflow_commons["log_dir"])

            # zip all files
            result = _zip_files(workflow_commons["prod_job_id"], selectedFiles)
            if not result["OK"]:
                job_report.setApplicationStatus("Failed to create zip of log files")
                return

            zip_file_name = result["Value"]

            # Instantiate the failover transfer client with the global request object
            if not failover_transfer:
                failover_transfer = FailoverTransfer(request)

            # logFilePath is something like /lhcb/MC/2016/LOG/00095376/0000/
            # the zipFileName should have the same name, e.g. 00000381.zip
            zipPath = os.path.join(workflow_commons["log_file_path"], zip_file_name)
            logHttpsURL = _get_log_url(log_se, zipPath)

            res = returnSingleResult(StorageElement(log_se).putFile({zipPath: zip_file_name}))
            if not res["OK"]:
                result = _uploadLogToFailoverSE(
                    failover_transfer, zip_file_name, log_lfn_path, workflow_commons["site_name"]
                )

                if not result["OK"]:
                    job_report.setApplicationStatus("Failed To Upload Logs")
                else:
                    uploadedSE = result["Value"]["uploadedSE"]
                    request = failover_transfer.request
                    _createLogUploadRequest(request, log_se, log_lfn_path, uploadedSE)

            # While it's the zip file that is uploaded, we set in job parameters its directory,
            # as the .zip is deflated automatically
            job_report.setJobParameter(
                "Log URL", f"<a href=\"{logHttpsURL.replace('.zip','/')}\">Log file directory</a>"
            )

        except Exception as e:
            failed = True
            raise WorkflowProcessingException(e) from e

        finally:
            save_workflow_commons(workflow_commons, workflow_commons_path, request, failed=failed)
