"""Test for the FailoverRequest command class."""

import json
from pathlib import Path

import pytest
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.TransformationSystem.Client.FileReport import FileReport
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRACCommon.Core.Utilities.ReturnValues import S_ERROR, S_OK
from pytest_mock import MockerFixture

from dirac_cwl.commands import FailoverRequest
from dirac_cwl.commands.workflow_commons import WorkflowCommons


class TestFailoverRequest:
    """Collection of tests for the FailoverRequest command."""

    @pytest.fixture
    def failover_request(self, mocker: MockerFixture, wf_commons, job_path):
        """FailoverRequest mocked command.

        Cleans created files after execution.
        """
        mocker.patch(
            "DIRAC.RequestManagementSystem.private.RequestValidator.RequestValidator.validate", return_value=S_OK()
        )

        command = FailoverRequest()
        command.request = Request()
        command.file_report = FileReport()
        command.failover_transfer = FailoverTransfer(command.request)
        command.job_report = JobReport(wf_commons["job_id"])

        mocker.patch.object(command.file_report, "setFileStatus")
        mocker.patch.object(command.job_report, "setApplicationStatus")

        yield command

        Path(job_path).joinpath("workflow_commons.json").unlink(missing_ok=True)

    def test_failoverRequest_success(self, mocker: MockerFixture, failover_request, job_path, wf_commons, request_file):
        """Test successful execution of FailoverRequest module."""
        problematic_files = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000287_1.ew.dst",
        ]

        mocker.patch(
            "DIRAC.TransformationSystem.Client.FileReport.FileReport.getFiles", side_effect=[problematic_files, []]
        )
        mocker.patch("DIRAC.TransformationSystem.Client.FileReport.FileReport.commit", return_value=S_OK("Anything"))

        wf_commons["inputs"] = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
            "/lhcb/data/2011/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
        ] + problematic_files

        WorkflowCommons(**wf_commons).save(job_path)

        failover_request.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        # Check the FileReport calls: the problematic file should not appear
        # The input files should be set to "Processed"
        assert failover_request.file_report.setFileStatus.call_count == 2
        args = failover_request.file_report.setFileStatus.call_args_list
        assert args[0][0][0] == int(updated_wf_commons.production_id)
        assert args[0][0][1] == updated_wf_commons.inputs[0]
        assert args[0][0][2] == "Processed"

        assert args[1][0][0] == int(updated_wf_commons.production_id)
        assert args[1][0][1] == updated_wf_commons.inputs[1]
        assert args[1][0][2] == "Processed"

        # Make sure the appliction is successfully finished
        assert failover_request.job_report.setApplicationStatus.call_count == 1
        assert failover_request.job_report.setApplicationStatus.call_args[0][0] == "Job Finished Successfully"

        # Make sure the forward DISET is not generated
        operations = json.loads(failover_request.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 0

        # Make sure the request json does not exists
        assert not Path(request_file).exists()

    def test_failoverRequest_commitFailure1(
        self, mocker: MockerFixture, failover_request, job_path, wf_commons, request_file
    ):
        """Test execution of FailoverRequest module when the fileReport.commit() fails.

        In this context, the second call to commit() will work, so the request should not be generated.
        """
        problematic_files = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000287_1.ew.dst",
        ]
        # Both calla to getFiles() will return the problematic files because the commit did not work
        mocker.patch(
            "DIRAC.TransformationSystem.Client.FileReport.FileReport.getFiles",
            side_effect=[problematic_files, problematic_files],
        )
        mocker.patch(
            "DIRAC.TransformationSystem.Client.FileReport.FileReport.commit", side_effect=[S_ERROR("Error"), S_OK(None)]
        )

        wf_commons["inputs"] = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
            "/lhcb/data/2011/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
        ] + problematic_files

        WorkflowCommons(**wf_commons).save(job_path)

        failover_request.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        # Check the FileReport calls: the problematic file should not appear
        # The input files should be set to "Processed"
        assert failover_request.file_report.setFileStatus.call_count == 2
        args = failover_request.file_report.setFileStatus.call_args_list
        assert args[0][0][0] == int(updated_wf_commons.production_id)
        assert args[0][0][1] == updated_wf_commons.inputs[0]
        assert args[0][0][2] == "Processed"

        assert args[1][0][0] == int(updated_wf_commons.production_id)
        assert args[1][0][1] == updated_wf_commons.inputs[1]
        assert args[1][0][2] == "Processed"

        # Make sure the appliction is successfully finished
        assert failover_request.job_report.setApplicationStatus.call_count == 1
        assert failover_request.job_report.setApplicationStatus.call_args[0][0] == "Job Finished Successfully"

        # Make sure the forward DISET is generated
        operations = json.loads(failover_request.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 0

        # Make sure the request json does not exists
        assert not Path(request_file).exists()

    def test_failoverRequest_commitFailure2(
        self, mocker: MockerFixture, failover_request, job_path, wf_commons, request_file
    ):
        """Test execution of FailoverRequest module when the fileReport.commit() fails.

        In this context, the second call to commit() will fail, so the request should be generated.
        """
        problematic_files = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000287_1.ew.dst",
        ]
        # Both calla to getFiles() will return the problematic files because the commit did not work
        mocker.patch(
            "DIRAC.TransformationSystem.Client.FileReport.FileReport.getFiles",
            side_effect=[problematic_files, problematic_files],
        )

        mocker.patch(
            "DIRAC.TransformationSystem.Client.FileReport.FileReport.commit",
            side_effect=[S_ERROR("Error"), S_ERROR("Error")],
        )

        wf_commons["inputs"] = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
            "/lhcb/data/2011/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
        ] + problematic_files

        WorkflowCommons(**wf_commons).save(job_path)

        failover_request.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        # Check the FileReport calls: the problematic file should not appear
        # The input files should be set to "Processed"
        assert failover_request.file_report.setFileStatus.call_count == 2
        args = failover_request.file_report.setFileStatus.call_args_list
        assert args[0][0][0] == int(updated_wf_commons.production_id)
        assert args[0][0][1] == updated_wf_commons.inputs[0]
        assert args[0][0][2] == "Processed"

        assert args[1][0][0] == int(updated_wf_commons.production_id)
        assert args[1][0][1] == updated_wf_commons.inputs[1]
        assert args[1][0][2] == "Processed"

        # Make sure the appliction is successfully finished
        assert failover_request.job_report.setApplicationStatus.call_count == 1
        assert failover_request.job_report.setApplicationStatus.call_args[0][0] == "Job Finished Successfully"

        # Make sure the forward DISET is generated
        operations = json.loads(failover_request.request.toJSON()["Value"])["Operations"]

        assert len(operations) == 1
        assert operations[0]["Type"] == "SetFileStatus"

        # Make sure the request json does not exists
        assert Path(request_file).exists()

    def test_failoverRequest_previousError_fail(
        self, mocker: MockerFixture, failover_request, job_path, wf_commons, request_file
    ):
        """Test FailoverRequest with an intentional failure."""
        problematic_files = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000287_1.ew.dst",
        ]
        mocker.patch(
            "DIRAC.TransformationSystem.Client.FileReport.FileReport.getFiles",
            side_effect=[problematic_files, problematic_files],
        )
        mocker.patch(
            "DIRAC.TransformationSystem.Client.FileReport.FileReport.commit",
            side_effect=[S_ERROR("Error"), S_OK("Error")],
        )

        wf_commons["inputs"] = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
            "/lhcb/data/2011/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
        ] + problematic_files

        # Intentional error
        wf_commons["step_status"] = "Failed"

        WorkflowCommons(**wf_commons).save(job_path)

        failover_request.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        # Check the FileReport calls: the problematic file should not appear
        # The input files should be set to "Unused"
        assert failover_request.file_report.setFileStatus.call_count == 2
        args = failover_request.file_report.setFileStatus.call_args_list
        assert args[0][0][0] == int(updated_wf_commons.production_id)
        assert args[0][0][1] == updated_wf_commons.inputs[0]
        assert args[0][0][2] == "Unused"

        assert args[1][0][0] == int(updated_wf_commons.production_id)
        assert args[1][0][1] == updated_wf_commons.inputs[1]
        assert args[1][0][2] == "Unused"

        # Make sure the appliction is not reported as a success
        assert failover_request.job_report.setApplicationStatus.call_count == 0

        # Make sure the forward DISET is not generated
        operations = json.loads(failover_request.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 0

        # Make sure the request json does not exists
        assert not Path(request_file).exists()
