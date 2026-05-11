"""LHCb command for registering the outputs generated to the corresponding SE or the FailoverSE in case of failure."""

import os
import random

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.TransformationSystem.Client.FileReport import FileReport
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs
from LHCbDIRAC.Core.Utilities.ResolveSE import getDestinationSEList
from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import getFileDescendents
from LHCbDIRAC.Workflow.Modules.UploadOutputData import (
    _createMetaDict,
    _getBKFiles,
    _getCleanRequest,
    _getFileMetada,
    _registerLFNs,
    _resolveSEs,
    _sendBKReport,
)

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .core import PostProcessCommand
from .utils import prepare_lhcb_workflow_commons, save_workflow_commons


class UploadOutputData(PostProcessCommand):
    """Registers every output generated to the corresponding SE and Catalog or to the FailoverSE in case of failure."""

    def execute(self, job_path, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        failed = False
        workflow_commons = {}
        request = None
        try:
            workflow_commons_path = kwargs.get("workflow_commons_path", os.path.join(job_path, "workflow_commons.json"))

            workflow_commons = prepare_lhcb_workflow_commons(
                workflow_commons_path,
                extra_mandatory_values=["output_data_step", "output_SEs"],
                extra_default_values={
                    "file_descendants": None,
                    "prod_output_LFNs": None,
                    "run_number": "Unknown",
                    "output_mode": "Any",
                },
            )
            request = Request(workflow_commons["request_dict"])

            if not workflow_commons["step_status"]["OK"]:
                return

            bk_client = BookkeepingClient()
            data_manager = DataManager()

            failover_se_list = getDestinationSEList("Tier1-Failover", workflow_commons["site_name"], outputmode="Any")
            random.shuffle(failover_se_list)

            file_report = FileReport()
            file_report.statusDict = workflow_commons["file_report_files_dict"]

            job_report = JobReport(workflow_commons["job_id"])

            if not workflow_commons["prod_output_LFNs"]:
                parameters = {
                    "PRODUCTION_ID": workflow_commons["production_id"],
                    "JOB_ID": workflow_commons["job_id"],
                    "configVersion": workflow_commons["config_version"],
                    "outputList": workflow_commons["outputs"],
                    "configName": workflow_commons["config_name"],
                    "outputDataFileMask": workflow_commons["output_data_file_mask"],
                }
                result = constructProductionLFNs(parameters, bk_client)

                if not result["OK"]:
                    raise WorkflowProcessingException("Unable to construsct production LFNs")

                workflow_commons["prod_output_LFNs"] = result["Value"]["ProductionOutputData"]

            file_metadata = _getFileMetada(
                workflow_commons["outputs"],
                workflow_commons["prod_output_LFNs"],
                workflow_commons["output_data_file_mask"],
                workflow_commons["output_data_step"],
                workflow_commons["output_SEs"],
            )

            if not file_metadata:
                return

            final = _resolveSEs(
                file_metadata,
                None,
                workflow_commons["site_name"],
                workflow_commons["output_mode"],
                workflow_commons["run_number"],
            )

            if workflow_commons["inputs"]:
                lfns_with_descendants = workflow_commons["file_descendants"]

                if not lfns_with_descendants:
                    lfns_with_descendants = getFileDescendents(
                        workflow_commons["production_id"],
                        workflow_commons["inputs"],
                        dm=data_manager,
                        bkClient=bk_client,
                    )

                if lfns_with_descendants:
                    file_report.setFileStatus(
                        int(workflow_commons["production_id"]), lfns_with_descendants, "Processed"
                    )
                    raise WorkflowProcessingException("Input Data Already Processed")

            bkFiles = _getBKFiles()

            for bkFile in bkFiles:
                with open(bkFile) as fd:
                    bkXML = fd.read()

                result = _sendBKReport(bk_client, request, bkXML)

            failover_transfer = FailoverTransfer(request)

            perform_bk_registration = []

            failover = {}
            for file_name, metadata in final.items():
                targetSE = metadata["resolvedSE"]
                file_meta_dict = _createMetaDict(metadata)
                result = failover_transfer.transferAndRegisterFile(
                    fileName=file_name,
                    localPath=metadata["localpath"],
                    lfn=metadata["filedict"]["LFN"],
                    destinationSEList=targetSE,
                    fileMetaDict=file_meta_dict,
                    masterCatalogOnly=True,
                )
                if not result["OK"]:
                    failover[file_name] = metadata
                else:
                    perform_bk_registration.append(metadata)

            cleanUp = False
            for file_name, metadata in failover.items():
                random.shuffle(failover_se_list)
                targetSE = metadata["resolvedSE"][0]
                metadata["resolvedSE"] = failover_se_list

                file_meta_dict = _createMetaDict(metadata)
                result = failover_transfer.transferAndRegisterFileFailover(
                    fileName=file_name,
                    localPath=metadata["localpath"],
                    lfn=metadata["filedict"]["LFN"],
                    targetSE=targetSE,
                    failoverSEList=metadata["resolvedSE"],
                    fileMetaDict=file_meta_dict,
                    masterCatalogOnly=True,
                )
                if not result["OK"]:
                    cleanUp = True
                    break

            request = failover_transfer.request
            if cleanUp:
                request = _getCleanRequest(request, final)
                raise WorkflowProcessingException("Failed to upload output data")

            if final:
                report = ", ".join(final)
                job_report.setJobParameter("UploadedOutputData", report)

            if perform_bk_registration:
                result = _registerLFNs(request, perform_bk_registration)
                if not result["OK"]:
                    raise WorkflowProcessingException(result["Message"])

        except:
            failed = True

        finally:
            save_workflow_commons(workflow_commons, workflow_commons_path, request=request, failed=failed)
