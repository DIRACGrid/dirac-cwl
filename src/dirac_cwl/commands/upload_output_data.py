"""LHCb command for registering the outputs generated to the corresponding SE or the FailoverSE in case of failure."""

import logging
import os
import random
import time

from DIRAC.Core.Utilities.ReturnValues import SErrorException, returnValueOrRaise
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

logger = logging.getLogger(__name__)


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
                try:
                    prod_lfn_dict = returnValueOrRaise(constructProductionLFNs(parameters, workflow_commons.bk_client))
                except SErrorException as e:
                    raise WorkflowProcessingException("Unable to construct production LFNs") from e

                workflow_commons.prod_output_lfns = prod_lfn_dict["ProductionOutputData"]

            file_metadata = _getFileMetada(
                workflow_commons.outputs,
                workflow_commons.prod_output_lfns,
                workflow_commons.output_data_file_mask,
                workflow_commons.output_data_step,
                workflow_commons.output_SEs,
            )

            if not file_metadata:
                logger.info("No output data files were determined to be uploaded for this workflow")
                return  # Does not fail

            final = _resolveSEs(
                file_metadata,
                None,
                workflow_commons.site_name,
                workflow_commons.output_mode,
                workflow_commons.run_number,
            )
            logger.info("The following files will be uploaded: %s", ", ".join(final))

            for fileName, metadata in final.items():
                logger.info("--------%s--------", fileName)
                for name, val in metadata.items():
                    logger.info("%s = %s", name, val)

            if workflow_commons.inputs:
                lfns_with_descendents = workflow_commons.file_descendents

                if not lfns_with_descendents:
                    lfns_with_descendents = getFileDescendents(
                        workflow_commons.production_id,
                        workflow_commons.inputs,
                        dm=workflow_commons.data_manager,
                        bkClient=workflow_commons.bk_client,
                    )

                if not lfns_with_descendents:
                    logger.info("No descendants found, outputs can be uploaded")
                else:
                    logger.error("Found descendants!!! Outputs won't be uploaded")
                    logger.info("Files with descendants: %s", " % ".join(lfns_with_descendents))
                    logger.info(
                        "The files above will be set as 'Processed', other lfns in input will be later reset as Unused"
                    )

                    workflow_commons.file_report.setFileStatus(
                        int(workflow_commons.production_id), lfns_with_descendents, "Processed"
                    )
                    raise WorkflowProcessingException("Input Data Already Processed")

            bkFiles = _getBKFiles()
            logger.info("The following BK records will be sent\n%s", ", ".join(bkFiles))

            for bkFile in bkFiles:
                with open(bkFile) as fd:
                    bkXML = fd.read()

                logger.info("Sending BK record:\n%s", bkXML)
                try:
                    returnValueOrRaise(_sendBKReport(workflow_commons.bk_client, workflow_commons.request, bkXML))
                    logger.info("Bookkeeping report sent for %s", bkFile)
                except SErrorException as e:
                    logger.error("Could not send Bookkeeping XML file to server:\n%s", e)
                    logger.info("Preparing DISET request for %s", bkFile)

            logger.info("Creating DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK in order to disable the Watchdog")
            with open("DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK", "w") as f:
                f.write(f"{time.asctime()}")

            perform_bk_registration = []

            failover = {}
            for file_name, metadata in final.items():
                targetSE = metadata["resolvedSE"]

                logger.info(
                    "Attempting to store file to SE %s to the following SE(s):\n%s", fileName, ", ".join(targetSE)
                )

                file_meta_dict = _createMetaDict(metadata)

                try:
                    returnValueOrRaise(
                        workflow_commons.failover_request.transferAndRegisterFile(
                            fileName=file_name,
                            localPath=metadata["localpath"],
                            lfn=metadata["filedict"]["LFN"],
                            destinationSEList=targetSE,
                            fileMetaDict=file_meta_dict,
                            masterCatalogOnly=True,
                        )
                    )
                    perform_bk_registration.append(metadata)
                    logger.info("File uploaded, will be registered in BK if all files uploaded for job %s", fileName)

                except SErrorException:
                    logger.error("Could not transfer and register %s with metadata:\n %s", fileName, metadata)
                    failover[file_name] = metadata

            clean_up = False
            for file_name, metadata in failover.items():
                logger.info(
                    "Setting default catalog for %s failover transfer registration to master catalog", file_name
                )

                random.shuffle(failover_se_list)
                targetSE = metadata["resolvedSE"][0]
                metadata["resolvedSE"] = failover_se_list

                file_meta_dict = _createMetaDict(metadata)
                try:
                    returnValueOrRaise(
                        workflow_commons.failover_request.transferAndRegisterFileFailover(
                            fileName=file_name,
                            localPath=metadata["localpath"],
                            lfn=metadata["filedict"]["LFN"],
                            targetSE=targetSE,
                            failoverSEList=metadata["resolvedSE"],
                            fileMetaDict=file_meta_dict,
                            masterCatalogOnly=True,
                        )
                    )
                except SErrorException:
                    logger.error(
                        "Could not transfer and register %s in failover with metadata:\n %s", fileName, metadata
                    )
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
                returnValueOrRaise(_registerLFNs(workflow_commons.request, perform_bk_registration))

        except WorkflowProcessingException:
            failed = True
            raise

        except Exception as e:
            logger.exception("Exception in UploadOutputData", exc_info=e)

            failed = True
            if workflow_commons:
                workflow_commons.job_report.setApplicationStatus(repr(e))

            raise WorkflowProcessingException(e) from e

        finally:
            if workflow_commons:
                workflow_commons.save(job_path, failed=failed)
