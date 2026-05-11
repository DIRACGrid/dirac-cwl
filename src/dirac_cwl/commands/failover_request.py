"""LHCb command for committing the status of the files in the file report.

The status will be "Processed" if everything ended properly or "Unused" if it did not.
"""

import json
import os

from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.TransformationSystem.Client.FileReport import FileReport
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from LHCbDIRAC.Workflow.Modules.FailoverRequest import _prepareRequest

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .core import PostProcessCommand
from .utils import prepare_lhcb_workflow_commons, save_workflow_commons


class FailoverRequest(PostProcessCommand):
    """Commits the status of the files in the file report.

    The status will be "Processed" if everything ended properly or "Unused" if it did not.
    """

    def execute(self, job_path, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        failed = False
        workflow_commons = {}
        try:
            workflow_commons_path = kwargs.get("workflow_commons_path", os.path.join(job_path, "workflow_commons.json"))

            workflow_commons = prepare_lhcb_workflow_commons(
                workflow_commons_path,
                extra_mandatory_values=[],
                extra_default_values={"accounting_registers": None},
            )

            request = Request(workflow_commons["request_dict"])
            file_report = FileReport()
            file_report.statusDict = workflow_commons["file_report_files_dict"]

            job_report = JobReport(workflow_commons["job_id"])

            _prepareRequest(request, workflow_commons["job_id"])

            filesInFileReport = file_report.getFiles()

            for lfn in workflow_commons["inputs"]:
                if lfn not in filesInFileReport:
                    status = "Processed" if workflow_commons["step_status"]["OK"] else "Unused"
                    file_report.setFileStatus(int(workflow_commons["production_id"]), lfn, status)

            file_report.commit()

            if workflow_commons["step_status"]["OK"]:
                if file_report.getFiles():
                    result = file_report.generateForwardDISET()
                    if result["OK"] and result["Value"]:
                        request.addOperation(result["Value"])

                job_report.setApplicationStatus("Job Finished Successfully", True)

            self.generateFailoverFile(job_report, request, workflow_commons)

        except Exception as e:
            failed = True
            raise WorkflowProcessingException(e) from e

        finally:
            save_workflow_commons(workflow_commons, workflow_commons_path, request=request, failed=failed)

    def generateFailoverFile(self, job_report, request, workflow_commons):
        """Create a request.json file."""
        result = job_report.generateForwardDISET()

        if result["OK"]:
            if result["Value"]:
                request.addOperation(result["Value"])

        if len(request):
            # Try to optimize the request
            try:
                request.optimize()
            except:  # noqa: E722
                pass

            # Validate request
            result = RequestValidator().validate(request)
            if not result["OK"]:
                raise WorkflowProcessingException(
                    "Failed to generate FailoverFile. Invalid request object", result["Message"]
                )

            # Get the request as a Json
            result = request.toJSON()
            if not result["OK"]:
                raise WorkflowProcessingException(result["Message"])

            # Write it
            fname = f"{workflow_commons['production_id']}_{workflow_commons['prod_job_id']}_request.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(result["Value"], f)

        if workflow_commons["accounting_registers"]:
            dsc = DataStoreClient()
            for register in workflow_commons["accounting_registers"]:
                dsc.addRegister(register)
            dsc.commit()
