"""Test for the UploadOutputDataFile command class."""

import json
from pathlib import Path

import pytest
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.TransformationSystem.Client.FileReport import FileReport
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRACCommon.Core.Utilities.ReturnValues import S_ERROR, S_OK
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from pytest_mock import MockerFixture

from dirac_cwl.commands import UploadOutputData
from dirac_cwl.commands.workflow_commons import StepStatus, WorkflowCommons
from dirac_cwl.core.exceptions import WorkflowProcessingException


class TestUploadOutputDataFile:
    """Collection of tests for the UploadOutputData command."""

    OUTPUT_DATA_STEP = "1"

    @pytest.fixture
    def sim_file(self, wf_commons, job_path):
        """Sim result file fixture."""
        path = Path(job_path).joinpath(
            f"{wf_commons['production_id']}_{wf_commons['prod_job_id']}_{self.OUTPUT_DATA_STEP}.sim"
        )

        with open(path, "w") as f:
            f.write("Bookkeeping file content")

        yield str(path.name)

        path.unlink(missing_ok=True)

    @pytest.fixture
    def bk_file(self, wf_commons, job_path):
        """Bookkeeping file fixture."""
        path = Path(job_path).joinpath(f"bookkeeping_{wf_commons['production_id']}_{wf_commons['prod_job_id']}.xml")

        with open(path, "w") as f:
            f.write("Sim file content")

        yield str(path)

        path.unlink(missing_ok=True)

    @pytest.fixture
    def upload_output(self, mocker: MockerFixture, wf_commons, job_path):
        """Fixture for UploadOutputData module."""
        mocker.patch("dirac_cwl.commands.upload_output_data.getDestinationSEList", return_value=["CERN", "CNAF"])
        mocker.patch("LHCbDIRAC.Workflow.Modules.UploadOutputData.getDestinationSEList", return_value=["CERN", "CNAF"])

        # Mock FileCatalog
        mocker.patch("DIRAC.Resources.Catalog.FileCatalog.FileCatalog.__init__", return_value=None)
        mocker.patch("DIRAC.Resources.Catalog.FileCatalog.FileCatalog.__getattr__", return_value=lambda x: S_OK({}))

        if "ProductionOutputData" in wf_commons:
            wf_commons.pop("ProductionOutputData")

        command = UploadOutputData()
        command.request = Request()
        command.failover_transfer = FailoverTransfer(command.request)
        command.bk_client = BookkeepingClient()
        command.file_report = FileReport()
        command.job_report = JobReport(wf_commons["job_id"])

        mocker.patch.object(command.bk_client, "sendXMLBookkeepingReport", return_value=S_OK())
        mocker.patch.object(command.file_report, "setFileStatus")
        mocker.patch.object(command.job_report, "setJobParameter")

        yield command

        Path(job_path).joinpath("workflow_commons.json").unlink(missing_ok=True)
        Path(job_path).joinpath("DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK").unlink(missing_ok=True)

    # Test Scenarios
    def test_uploadOutputData_success(
        self, mocker: MockerFixture, upload_output, job_path, wf_commons, sim_file, bk_file
    ):
        """Test successful execution of UploadOutputData module.

        * The output should be uploaded and registered in the bookkeeping system.
        * The bookkeeping report should be sent and the job parameter updated.
        """
        mocker.patch.object(
            upload_output.failover_transfer,
            "transferAndRegisterFile",
            return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file}),
        )

        mocker.patch.object(upload_output.failover_transfer, "transferAndRegisterFileFailover")

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": ["Tier1-Buffer"],
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        WorkflowCommons(**wf_commons).save(job_path)

        upload_output.execute(job_path)

        assert upload_output.file_report.setFileStatus.call_count == 0
        assert upload_output.bk_client.sendXMLBookkeepingReport.call_count == 1

        assert upload_output.failover_transfer.transferAndRegisterFile.call_count == 1
        assert upload_output.failover_transfer.transferAndRegisterFile.call_args[1]["fileName"] == str(sim_file)

        assert upload_output.failover_transfer.transferAndRegisterFileFailover.call_count == 0

        assert upload_output.job_report.setJobParameter.call_count == 1
        assert upload_output.job_report.setJobParameter.call_args[0][0] == "UploadedOutputData"
        assert upload_output.job_report.setJobParameter.call_args[0][1] == str(sim_file)

        # Make sure the forward DISET is not generated
        operations = json.loads(upload_output.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 0

    def test_uploadOutputData_failedBKRegistration(
        self, mocker: MockerFixture, upload_output, job_path, wf_commons, sim_file, bk_file
    ):
        """Test execution of UploadOutputData module when the BK registation fails.

        * The output should be uploaded but not registered in the bookkeeping system now.
        """
        mocker.patch.object(
            upload_output.failover_transfer,
            "transferAndRegisterFile",
            return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file}),
        )
        mocker.patch.object(upload_output.failover_transfer, "transferAndRegisterFileFailover")

        # BK registration failure
        mocker.patch(
            "DIRAC.Resources.Catalog.FileCatalog.FileCatalog.__getattr__",
            return_value=lambda x: S_OK(
                {
                    "Failed": {
                        f"/lhcb/{wf_commons['config_name']}/{wf_commons['config_version']}/"
                        f"SIM/00000{wf_commons['production_id']}/0000/{sim_file}": "error"
                    }
                }
            ),
        )

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": ["Tier1-Buffer"],
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        WorkflowCommons(**wf_commons).save(job_path)

        upload_output.execute(job_path)

        assert upload_output.file_report.setFileStatus.call_count == 0
        assert upload_output.bk_client.sendXMLBookkeepingReport.call_count == 1

        assert upload_output.failover_transfer.transferAndRegisterFile.call_count == 1
        assert upload_output.failover_transfer.transferAndRegisterFile.call_args[1]["fileName"] == str(sim_file)

        assert upload_output.failover_transfer.transferAndRegisterFileFailover.call_count == 0

        assert upload_output.job_report.setJobParameter.call_count == 1
        assert upload_output.job_report.setJobParameter.call_args[0][0] == "UploadedOutputData"
        assert upload_output.job_report.setJobParameter.call_args[0][1] == str(sim_file)

        # Make sure the request is generated
        operations = json.loads(upload_output.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 1

        assert operations[0]["Type"] == "RegisterFile"
        assert operations[0]["Catalog"] == "BookkeepingDB"
        assert sim_file in operations[0]["Files"][0]["LFN"]

    def test_uploadOutputData_postponeBKRegistration(
        self, mocker: MockerFixture, upload_output, job_path, wf_commons, sim_file, bk_file
    ):
        """Test execution of UploadOutputData module when there is already a RegisterFile operation on the output.

        * The output should be uploaded but not registered in the bookkeeping system now.
        """
        mocker.patch.object(
            upload_output.failover_transfer,
            "transferAndRegisterFile",
            return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file}),
        )

        mocker.patch.object(upload_output.failover_transfer, "transferAndRegisterFileFailover")

        # Mock a previous failover request: the BK registration should be postponed and added to the request
        file1 = File()
        file1.LFN = (
            f"/lhcb/{wf_commons['config_name']}/{wf_commons['config_version']}"
            f"/SIM/00000{wf_commons['production_id']}/0000/{sim_file}"
        )
        o1 = Operation()
        o1.Type = "RegisterFile"
        o1.addFile(file1)
        upload_output.request.addOperation(o1)

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": ["Tier1-Buffer"],
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        WorkflowCommons(**wf_commons).save(job_path)

        # Execute module
        upload_output.execute(job_path)

        assert upload_output.file_report.setFileStatus.call_count == 0
        assert upload_output.bk_client.sendXMLBookkeepingReport.call_count == 1

        assert upload_output.failover_transfer.transferAndRegisterFile.call_count == 1
        assert upload_output.failover_transfer.transferAndRegisterFile.call_args[1]["fileName"] == str(sim_file)

        assert upload_output.failover_transfer.transferAndRegisterFileFailover.call_count == 0

        assert upload_output.job_report.setJobParameter.call_count == 1
        assert upload_output.job_report.setJobParameter.call_args[0][0] == "UploadedOutputData"
        assert upload_output.job_report.setJobParameter.call_args[0][1] == str(sim_file)

        # Make sure the request is generated
        operations = json.loads(upload_output.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 2

        assert operations[0]["Type"] == "RegisterFile"
        assert operations[0]["Catalog"] is None
        assert sim_file in operations[0]["Files"][0]["LFN"]

        assert operations[1]["Type"] == "RegisterFile"
        assert operations[1]["Catalog"] == "BookkeepingDB"
        assert sim_file in operations[1]["Files"][0]["LFN"]

    def test_uploadOutputData_errorBKRegistration(
        self, mocker: MockerFixture, upload_output, job_path, wf_commons, sim_file, bk_file
    ):
        """Test execution of UploadOutputData module when an error occurs during the BK registation.

        * The output should be uploaded but not registered in the bookkeeping system at all.
        """
        mocker.patch.object(
            upload_output.failover_transfer,
            "transferAndRegisterFile",
            return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file}),
        )

        mocker.patch.object(upload_output.failover_transfer, "transferAndRegisterFileFailover")

        # BK registration failure
        mocker.patch(
            "DIRAC.Resources.Catalog.FileCatalog.FileCatalog.__getattr__",
            return_value=lambda x: S_ERROR("Error registering file"),
        )

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": ["Tier1-Buffer"],
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        # BK registration failure
        mocker.patch(
            "DIRAC.Resources.Catalog.FileCatalog.FileCatalog.__getattr__",
            return_value=lambda x: S_ERROR("Error registering file"),
        )

        WorkflowCommons(**wf_commons).save(job_path)

        with pytest.raises(WorkflowProcessingException, match="Could Not Perform BK Registration"):
            upload_output.execute(job_path)

        assert upload_output.file_report.setFileStatus.call_count == 0
        assert upload_output.bk_client.sendXMLBookkeepingReport.call_count == 1

        assert upload_output.failover_transfer.transferAndRegisterFile.call_count == 1
        assert upload_output.failover_transfer.transferAndRegisterFile.call_args[1]["fileName"] == str(sim_file)

        assert upload_output.failover_transfer.transferAndRegisterFileFailover.call_count == 0

        assert upload_output.job_report.setJobParameter.call_count == 1
        assert upload_output.job_report.setJobParameter.call_args[0][0] == "UploadedOutputData"
        assert upload_output.job_report.setJobParameter.call_args[0][1] == str(sim_file)

        # Make sure the request is generated
        operations = json.loads(upload_output.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 0

    def test_uploadOutputData_failUpload1(
        self, mocker: MockerFixture, upload_output, job_path, wf_commons, sim_file, bk_file
    ):
        """Test execution of UploadOutputData module when there is a 1st failure to upload outputs.

        * The output should be uploaded correctly with the second method.
        """
        mocker.patch.object(
            upload_output.failover_transfer,
            "transferAndRegisterFile",
            return_value=S_ERROR("Error uploading file"),
        )

        mocker.patch.object(
            upload_output.failover_transfer,
            "transferAndRegisterFileFailover",
            return_value=S_OK(),
        )

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": ["Tier1-Buffer"],
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        WorkflowCommons(**wf_commons).save(job_path)

        upload_output.execute(job_path)

        assert upload_output.file_report.setFileStatus.call_count == 0
        assert upload_output.bk_client.sendXMLBookkeepingReport.call_count == 1

        assert upload_output.failover_transfer.transferAndRegisterFile.call_count == 1
        assert upload_output.failover_transfer.transferAndRegisterFile.call_args[1]["fileName"] == str(sim_file)

        assert upload_output.failover_transfer.transferAndRegisterFileFailover.call_count == 1
        assert upload_output.failover_transfer.transferAndRegisterFileFailover.call_args[1]["fileName"] == str(sim_file)

        assert upload_output.job_report.setJobParameter.call_count == 1
        assert upload_output.job_report.setJobParameter.call_args[0][0] == "UploadedOutputData"
        assert upload_output.job_report.setJobParameter.call_args[0][1] == str(sim_file)

        # Make sure the request is not generated
        operations = json.loads(upload_output.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 0

    def test_uploadOutputData_failUpload2(
        self, mocker: MockerFixture, upload_output, job_path, wf_commons, sim_file, bk_file
    ):
        """Test execution of UploadOutputData module when there is a 2 failures to upload outputs.

        * A request should be generated to upload outputs later.
        """
        mocker.patch.object(
            upload_output.failover_transfer,
            "transferAndRegisterFile",
            return_value=S_ERROR("Error uploading file"),
        )

        mocker.patch.object(
            upload_output.failover_transfer,
            "transferAndRegisterFileFailover",
            return_value=S_ERROR("Error uploading file"),
        )

        # Mock a previous failover request:
        # Add the end of the execution, o1 should be removed
        file1 = File()
        file1.LFN = (
            f"/lhcb/{wf_commons['config_name']}/{wf_commons['config_version']}"
            f"/SIM/00000{wf_commons['production_id']}/0000/{sim_file}"
        )
        file2 = File()
        file2.LFN = "/another/file.txt"

        o1 = Operation()
        o1.Type = "RegisterFile"
        o1.addFile(file1)
        o2 = Operation()
        o2.Type = "RegisterFile"
        o2.addFile(file2)

        upload_output.request.addOperation(o1)
        upload_output.request.addOperation(o2)

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": ["Tier1-Buffer"],
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        WorkflowCommons(**wf_commons).save(job_path)

        # Execute module
        with pytest.raises(WorkflowProcessingException, match="Failed to upload output data"):
            upload_output.execute(job_path)

        assert upload_output.file_report.setFileStatus.call_count == 0
        assert upload_output.bk_client.sendXMLBookkeepingReport.call_count == 1

        assert upload_output.failover_transfer.transferAndRegisterFile.call_count == 1
        assert upload_output.failover_transfer.transferAndRegisterFile.call_args[1]["fileName"] == str(sim_file)

        assert upload_output.failover_transfer.transferAndRegisterFileFailover.call_count == 1
        assert upload_output.failover_transfer.transferAndRegisterFileFailover.call_args[1]["fileName"] == str(sim_file)

        assert upload_output.job_report.setJobParameter.call_count == 0

        # Make sure the request is generated
        operations = json.loads(upload_output.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 2

        assert operations[0]["Type"] == "RegisterFile"
        assert operations[0]["TargetSE"] is None
        assert operations[0]["SourceSE"] is None
        assert sim_file not in operations[0]["Files"][0]["LFN"]

        assert operations[1]["Type"] == "RemoveFile"
        assert operations[1]["TargetSE"] is None
        assert operations[1]["SourceSE"] is None
        assert sim_file in operations[1]["Files"][0]["LFN"]

    def test_uploadOutputData_BKReportError(
        self, mocker: MockerFixture, upload_output, job_path, wf_commons, sim_file, bk_file
    ):
        """Test execution of UploadOutputData module when the BK report cannot be sent.

        * The output should be uploaded and registered in the bookkeeping system.
        * The bookkeeping report should be added to a failover request.
        """
        mocker.patch.object(
            upload_output.failover_transfer,
            "transferAndRegisterFile",
            return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file}),
        )

        mocker.patch.object(
            upload_output.failover_transfer,
            "transferAndRegisterFileFailover",
            return_value=S_ERROR("Error uploading file"),
        )

        mocker.patch.object(
            upload_output.bk_client,
            "sendXMLBookkeepingReport",
            return_value={"OK": False, "rpcStub": "Error", "Message": "Error sending BK report"},
        )

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": ["Tier1-Buffer"],
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        WorkflowCommons(**wf_commons).save(job_path)

        upload_output.execute(job_path)

        assert upload_output.file_report.setFileStatus.call_count == 0
        assert upload_output.bk_client.sendXMLBookkeepingReport.call_count == 1

        assert upload_output.failover_transfer.transferAndRegisterFile.call_count == 1
        assert upload_output.failover_transfer.transferAndRegisterFile.call_args[1]["fileName"] == str(sim_file)

        assert upload_output.failover_transfer.transferAndRegisterFileFailover.call_count == 0

        assert upload_output.job_report.setJobParameter.call_count == 1
        assert upload_output.job_report.setJobParameter.call_args[0][0] == "UploadedOutputData"
        assert upload_output.job_report.setJobParameter.call_args[0][1] == str(sim_file)

        # Make sure the request is not generated
        operations = json.loads(upload_output.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 1

        assert operations[0]["Type"] == "ForwardDISET"

    def test_uploadOutputData_withDescendents(
        self, mocker: MockerFixture, upload_output, job_path, wf_commons, sim_file, bk_file
    ):
        """Test execution of UploadOutputData module when there is already file descendants.

        It means that the input data has already been processed.
        * The output should not be uploaded and registered in the bookkeeping system.
        * The bookkeeping report should not be sent.
        """
        mocker.patch(
            "dirac_cwl.commands.upload_output_data.getFileDescendents", return_value=S_OK(["/path/to/other/file.txt"])
        )

        mocker.patch.object(
            upload_output.failover_transfer,
            "transferAndRegisterFile",
            return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file}),
        )

        mocker.patch.object(upload_output.failover_transfer, "transferAndRegisterFileFailover")

        mocker.patch.object(upload_output.bk_client, "sendXMLBookkeepingReport")

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": ["Tier1-Buffer"],
        }
        wf_commons["inputs"] = ["AnyInputFile1"]
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        WorkflowCommons(**wf_commons).save(job_path)

        with pytest.raises(WorkflowProcessingException):
            upload_output.execute(job_path)

        assert upload_output.file_report.setFileStatus.assert_called_once
        assert upload_output.file_report.setFileStatus.call_args[0][0] == int(wf_commons["production_id"])
        assert upload_output.bk_client.sendXMLBookkeepingReport.call_count == 0

        assert upload_output.failover_transfer.transferAndRegisterFile.call_count == 0
        assert upload_output.failover_transfer.transferAndRegisterFileFailover.call_count == 0

        assert upload_output.job_report.setJobParameter.call_count == 0

        # Make sure the request is not generated
        operations = json.loads(upload_output.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 0

    def test_uploadOutputData_noOutput(self, mocker: MockerFixture, upload_output, job_path, wf_commons, sim_file):
        """Test UploadOutputData with no output data."""
        mocker.patch.object(
            upload_output.failover_transfer,
            "transferAndRegisterFile",
            return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file}),
        )

        mocker.patch.object(upload_output.failover_transfer, "transferAndRegisterFileFailover")

        mocker.patch.object(upload_output.bk_client, "sendXMLBookkeepingReport")

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": ["Tier1-Buffer"],
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        # Remove the output
        Path(job_path).joinpath(sim_file).unlink(missing_ok=True)

        WorkflowCommons(**wf_commons).save(job_path)

        # Execute module
        with pytest.raises(WorkflowProcessingException, match="Output data not found"):
            upload_output.execute(job_path)

        assert upload_output.file_report.setFileStatus.call_count == 0
        assert upload_output.bk_client.sendXMLBookkeepingReport.call_count == 0

        assert upload_output.failover_transfer.transferAndRegisterFile.call_count == 0
        assert upload_output.failover_transfer.transferAndRegisterFileFailover.call_count == 0

        assert upload_output.job_report.setJobParameter.call_count == 0

        # Make sure the request is not generated
        operations = json.loads(upload_output.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 0

    def test_uploadOutputData_previousError_fail(
        self, mocker: MockerFixture, upload_output, job_path, wf_commons, sim_file
    ):
        """Test UploadOutputData with an intentional failure."""
        mocker.patch.object(upload_output.failover_transfer, "transferAndRegisterFile")

        mocker.patch.object(upload_output.failover_transfer, "transferAndRegisterFileFailover")

        mocker.patch.object(upload_output.bk_client, "sendXMLBookkeepingReport")

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": ["Tier1-Buffer"],
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        wf_commons["step_status"] = StepStatus.Failed

        Path(sim_file).unlink(missing_ok=True)

        WorkflowCommons(**wf_commons).save(job_path)

        upload_output.execute(job_path)

        assert upload_output.file_report.setFileStatus.call_count == 0
        assert upload_output.bk_client.sendXMLBookkeepingReport.call_count == 0

        assert upload_output.failover_transfer.transferAndRegisterFile.call_count == 0
        assert upload_output.failover_transfer.transferAndRegisterFileFailover.call_count == 0

        assert upload_output.job_report.setJobParameter.call_count == 0

        # Make sure the request is not generated
        operations = json.loads(upload_output.request.toJSON()["Value"])["Operations"]
        assert len(operations) == 0
