"""LHCb command for registering the outputs generated to the corresponding SE or the FailoverSE in case of failure."""

import os
import random

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
from .workflow_commons import StepStatus, WorkflowCommons


class UploadOutputData(PostProcessCommand):
    """Registers every output generated to the corresponding SE and Catalog or to the FailoverSE in case of failure."""

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

            failover_se_list = getDestinationSEList("Tier1-Failover", workflow_commons.site_name, outputmode="Any")
            random.shuffle(failover_se_list)

            if not workflow_commons.prod_output_lfns:
                parameters = {
                    "PRODUCTION_ID": workflow_commons.production_id,
                    "JOB_ID": workflow_commons.job_id,
                    "configVersion": workflow_commons.config_version,
                    "outputList": workflow_commons.outputs,
                    "configName": workflow_commons.config_name,
                    "outputDataFileMask": workflow_commons.output_data_file_mask,
                }
                result = constructProductionLFNs(parameters, workflow_commons.bk_client)

                if not result["OK"]:
                    raise WorkflowProcessingException("Unable to construct production LFNs")

                workflow_commons.prod_output_lfns = result["Value"]["ProductionOutputData"]

            file_metadata = _getFileMetada(
                workflow_commons.outputs,
                workflow_commons.prod_output_lfns,
                workflow_commons.output_data_file_mask,
                workflow_commons.output_data_step,
                workflow_commons.output_SEs,
            )

            if not file_metadata:
                return

            final = _resolveSEs(
                file_metadata,
                None,
                workflow_commons.site_name,
                workflow_commons.output_mode,
                workflow_commons.run_number,
            )

            if workflow_commons.inputs:
                lfns_with_descendents = workflow_commons.file_descendents

                if not lfns_with_descendents:
                    lfns_with_descendents = getFileDescendents(
                        workflow_commons.production_id,
                        workflow_commons.inputs,
                        dm=workflow_commons.data_manager,
                        bkClient=workflow_commons.bk_client,
                    )

                if lfns_with_descendents:
                    workflow_commons.file_report.setFileStatus(
                        int(workflow_commons.production_id), lfns_with_descendents, "Processed"
                    )
                    raise WorkflowProcessingException("Input Data Already Processed")

            bkFiles = _getBKFiles()

            for bkFile in bkFiles:
                with open(bkFile) as fd:
                    bkXML = fd.read()

                result = _sendBKReport(workflow_commons.bk_client, workflow_commons.request, bkXML)

            perform_bk_registration = []

            failover = {}
            for file_name, metadata in final.items():
                targetSE = metadata["resolvedSE"]
                file_meta_dict = _createMetaDict(metadata)
                result = workflow_commons.failover_request.transferAndRegisterFile(
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

            clean_up = False
            for file_name, metadata in failover.items():
                random.shuffle(failover_se_list)
                targetSE = metadata["resolvedSE"][0]
                metadata["resolvedSE"] = failover_se_list

                file_meta_dict = _createMetaDict(metadata)
                result = workflow_commons.failover_request.transferAndRegisterFileFailover(
                    fileName=file_name,
                    localPath=metadata["localpath"],
                    lfn=metadata["filedict"]["LFN"],
                    targetSE=targetSE,
                    failoverSEList=metadata["resolvedSE"],
                    fileMetaDict=file_meta_dict,
                    masterCatalogOnly=True,
                )
                if not result["OK"]:
                    clean_up = True
                    break

            workflow_commons.request = workflow_commons.failover_request.request
            if clean_up:
                workflow_commons.request = _getCleanRequest(workflow_commons.request, final)
                raise WorkflowProcessingException("Failed to upload output data")

            if final:
                report = ", ".join(final)
                workflow_commons.job_report.setJobParameter("UploadedOutputData", report)

            if perform_bk_registration:
                result = _registerLFNs(workflow_commons.request, perform_bk_registration)
                if not result["OK"]:
                    raise WorkflowProcessingException(result["Message"])

        except Exception as e:
            failed = True
            raise WorkflowProcessingException(e) from e

        finally:
            if workflow_commons:
                workflow_commons.save(job_path, failed=failed)
