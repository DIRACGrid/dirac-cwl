"""LHCb command for committing the status of the files in the file report.

The status will be "Processed" if everything ended properly or "Unused" if it did not.
"""

import json
import logging
import os

from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.Core.Utilities.ReturnValues import SErrorException, returnValueOrRaise
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.TransformationSystem.Client.FileReport import FileReport
from LHCbDIRAC.Workflow.Modules.FailoverRequest import _prepareRequest

from .core import PostProcessCommand
from .workflow_commons import StepStatus, WorkflowCommons

logger = logging.getLogger(__name__)


class FailoverRequest(PostProcessCommand):
    """Commits the status of the files in the file report.

    The status will be "Processed" if everything ended properly or "Unused" if it did not.
    """

    def _execute(self, job_path: os.PathLike[str], workflow_commons: WorkflowCommons, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param workflow_commons: WorkflowCommons object.
        :param kwargs: Additional keyword arguments.
        """
        if not self.request:
            self.request = Request(workflow_commons.request_dict)

        _prepareRequest(self.request, workflow_commons.job_id)

        files_in_file_report = self.file_report.getFiles()

        for lfn in workflow_commons.inputs:
            if lfn not in files_in_file_report:
                status = "Processed" if workflow_commons.step_status == StepStatus.Done else "Unused"
                if status == "Unused":
                    logger.info("Set status of %s to 'Unused' due to workflow failure", lfn)
                else:
                    logger.debug("No status populated for %s, setting to 'Processed'", lfn)

                self.file_report.setFileStatus(int(workflow_commons.production_id), lfn, status)

        try:
            value = returnValueOrRaise(self.file_report.commit())
            if value:
                logger.info("Status of files have been properly updated in the TransformationDB")
            else:
                logger.warning("No file status update reported. There are no input files?")
        except SErrorException as e:
            logger.error("Something went wrong trying fileReport.commit() %s", e)

        if self.file_report.getFiles():
            logger.error("On first attempt, failed to report file status to TransformationDB")
            try:
                value = returnValueOrRaise(self.file_report.generateForwardDISET())
                if not value:
                    logger.info("On second attempt, files correctly reported to TransformationDB")
                elif workflow_commons.step_status == StepStatus.Done:
                    logger.info("Adding a SetFileStatus operation to the request")
                    self.request.addOperation(value)
                else:
                    logger.info("The job should fail: do not set requests, as the DRA will take care")
            except SErrorException as e:
                logger.warning("Could not generate Operation for file report: %s", e)

        if workflow_commons.step_status == StepStatus.Done:
            self.job_report.setApplicationStatus("Job Finished Successfully", True)

        self.generate_failover_file(workflow_commons)

    def _resolve_clients(self, workflow_commons: WorkflowCommons):
        super()._resolve_clients(workflow_commons)

        if not self.file_report:
            self.file_report = FileReport()

        if not self.dsc:
            self.dsc = DataStoreClient()

    def generate_failover_file(self, workflow_commons: WorkflowCommons):
        """Create a request.json file."""
        try:
            diset_op = returnValueOrRaise(self.job_report.generateForwardDISET())
        except SErrorException as e:
            logger.warning("Could not generate Operation for job report", exc_info=e)

        if diset_op:
            logger.info("Populating request with job report information")
            self.request.addOperation(diset_op)

        if len(self.request):
            # Try to optimize the request
            try:
                returnValueOrRaise(self.request.optimize())
            except SErrorException as e:
                logger.error("Could not optimize", exc_info=e)
                logger.error("Not failing the job because of that, keep going")
            except Exception:
                pass

            # Validate self.request
            returnValueOrRaise(RequestValidator().validate(self.request))

            # Get the self.request as a Json
            request_json_content = returnValueOrRaise(self.request.toJSON())

            # Write it
            fname = f"{workflow_commons.production_id}_{workflow_commons.prod_job_id}_request.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(request_json_content, f)

        if workflow_commons.accounting_registers:
            for register in workflow_commons.accounting_registers:
                self.dsc.addRegister(register)
            self.dsc.commit()
