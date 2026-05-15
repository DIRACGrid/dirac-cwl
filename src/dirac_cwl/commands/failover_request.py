"""LHCb command for committing the status of the files in the file report.

The status will be "Processed" if everything ended properly or "Unused" if it did not.
"""

import json
import os

from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from LHCbDIRAC.Workflow.Modules.FailoverRequest import _prepareRequest

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .core import PostProcessCommand
from .workflow_commons import StepStatus, WorkflowCommons


class FailoverRequest(PostProcessCommand):
    """Commits the status of the files in the file report.

    The status will be "Processed" if everything ended properly or "Unused" if it did not.
    """

    def execute(self, job_path: os.PathLike, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        failed = False
        workflow_commons = None
        try:
            workflow_commons = WorkflowCommons.load(job_path)

            _prepareRequest(workflow_commons.request, workflow_commons.job_id)

            filesInFileReport = workflow_commons.file_report.getFiles()

            for lfn in workflow_commons.inputs:
                if lfn not in filesInFileReport:
                    status = "Processed" if workflow_commons.step_status == StepStatus.Done else "Unused"
                    workflow_commons.file_report.setFileStatus(int(workflow_commons.production_id), lfn, status)

            workflow_commons.file_report.commit()

            if workflow_commons.step_status == StepStatus.Done:
                if workflow_commons.file_report.getFiles():
                    result = workflow_commons.file_report.generateForwardDISET()
                    if result["OK"] and result["Value"]:
                        workflow_commons.request.addOperation(result["Value"])

                workflow_commons.job_report.setApplicationStatus("Job Finished Successfully", True)

            self.generateFailoverFile(workflow_commons)

        except Exception as e:
            failed = True
            raise WorkflowProcessingException(e) from e

        finally:
            if workflow_commons:
                workflow_commons.save(job_path, failed=failed)

    def generateFailoverFile(self, workflow_commons: WorkflowCommons):
        """Create a workflow_commons.request.json file."""
        result = workflow_commons.job_report.generateForwardDISET()

        if result["OK"]:
            if result["Value"]:
                workflow_commons.request.addOperation(result["Value"])

        if len(workflow_commons.request):
            # Try to optimize the request
            try:
                workflow_commons.request.optimize()
            except Exception:  # noqa: E722
                pass

            # Validate workflow_commons.request
            result = RequestValidator().validate(workflow_commons.request)
            if not result["OK"]:
                raise WorkflowProcessingException(
                    "Failed to generate FailoverFile. Invalid workflow_commons.request object", result["Message"]
                )

            # Get the workflow_commons.request as a Json
            result = workflow_commons.request.toJSON()
            if not result["OK"]:
                raise WorkflowProcessingException(result["Message"])

            # Write it
            fname = f"{workflow_commons.production_id}_{workflow_commons.prod_job_id}_request.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(result["Value"], f)

        if workflow_commons.accounting_registers:
            for register in workflow_commons.accounting_registers:
                workflow_commons.dsc.addRegister(register)
            workflow_commons.dsc.commit()
