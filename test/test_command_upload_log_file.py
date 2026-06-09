"""Test for the UploadLogFile command class."""

import json
import shutil
import zipfile
from pathlib import Path

import pytest
from DIRACCommon.Core.Utilities.ReturnValues import S_ERROR, S_OK
from pytest_mock import MockerFixture

from dirac_cwl.commands import UploadLogFile
from dirac_cwl.commands.workflow_commons import WorkflowCommons


class TestUploadLogFile:
    """Collection of tests for the UploadLogFile command."""

    @pytest.fixture
    def uplogfile(self, mocker: MockerFixture, wf_commons, job_path):
        """Fixture for UploadLogFile module."""
        uplogfile = UploadLogFile()

        yield uplogfile

        Path(job_path).joinpath(f"{wf_commons['prod_job_id']}.zip").unlink(missing_ok=True)
        shutil.rmtree(Path(job_path).joinpath("unzipped"), ignore_errors=True)

        Path(job_path).joinpath("workflow_commons.json").unlink(missing_ok=True)

    @pytest.fixture
    def prodconf_json(self, job_path):
        """prodconf.json file fixture."""
        file = Path(job_path).joinpath("prodConf_example.json")

        with open(file, "w") as f:
            f.write('{"foo": "bar"}')

        yield file

        file.unlink(missing_ok=True)

    @pytest.fixture
    def prodconf_py(self, job_path):
        """prodconf.py file fixture."""
        file = Path(job_path).joinpath("prodConf_example.py")

        with open(file, "w") as f:
            f.write('foo = "bar"')

        yield file

        file.unlink(missing_ok=True)

    # Test Scenarios
    def test_uploadLogFile_success(
        self, mocker: MockerFixture, uplogfile, job_path, wf_commons, prodconf_json, prodconf_py
    ):
        """Test successful execution of UploadLogFile module."""
        log_url = "notImportant"
        mock_se_method = mocker.patch(
            "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__executeMethod",
            return_value=S_OK({"Failed": [], "Successful": {log_url: log_url}}),
        )
        mock_transferAndRegisterFile = mocker.patch(
            "DIRAC.DataManagementSystem.Client.FailoverTransfer.FailoverTransfer.transferAndRegisterFile",
            return_value=S_OK(),
        )
        mock_setJobParameter = mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setJobParameter")
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus"
        )

        WorkflowCommons(**wf_commons).save(job_path)

        uplogfile.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        # Check the log directory
        assert updated_wf_commons.log_dir != ""
        log_dir = Path(updated_wf_commons.log_dir)
        assert log_dir.exists()
        assert log_dir.is_dir()
        assert log_dir.joinpath(prodconf_json).exists()
        assert log_dir.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert log_dir.joinpath(prodconf_py).exists()
        assert log_dir.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        for file in log_dir.iterdir():
            assert file.stat().st_mode & 0o777 == 0o755

        # Check the generated zip file
        zipFile = Path(job_path).joinpath(f"{updated_wf_commons.prod_job_id}.zip")
        assert zipFile.exists()

        zipfile.ZipFile(zipFile, "r").extractall("unzipped")
        unzipped = Path("unzipped").joinpath(updated_wf_commons.prod_job_id)
        assert unzipped.joinpath(prodconf_json).exists()
        assert unzipped.joinpath(prodconf_py).exists()
        assert unzipped.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert unzipped.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        # Make sure that StorageElement was called twice (getURL, putFile)
        assert mock_se_method.call_count == 2

        # Make sure that the request was not created
        assert mock_transferAndRegisterFile.call_count == 0

        # Make sure the application status was not changed
        assert mock_setApplicationStatus.call_count == 0

        # Check the jobReport.setParameter arguments
        assert mock_setJobParameter.call_count == 1
        assert mock_setJobParameter.call_args_list
        params = mock_setJobParameter.call_args_list[0][0]
        assert params[0] == "Log URL"
        assert params[1] == f'<a href="{log_url}">Log file directory</a>'

        shutil.rmtree(updated_wf_commons.log_dir, ignore_errors=True)

    def test_uploadLogFile_noOutputFile(self, mocker: MockerFixture, uplogfile, job_path, wf_commons):
        """Test execution of UploadLogFile module when there is no output files.

        * populateLogDirectory should return an error, because there is no "successful" files in log_dir.
        """
        mock_se_method = mocker.patch(
            "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__executeMethod",
            return_value=S_OK({"Failed": [], "Successful": {"notImportant": "notImportant"}}),
        )
        mock_transferAndRegisterFile = mocker.patch(
            "DIRAC.DataManagementSystem.Client.FailoverTransfer.FailoverTransfer.transferAndRegisterFile",
            return_value=S_OK(),
        )
        mock_setJobParameter = mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setJobParameter")
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus"
        )

        WorkflowCommons(**wf_commons).save(job_path)

        uplogfile.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        # Check the log directory
        assert updated_wf_commons.log_dir != ""
        log_dir = Path(updated_wf_commons.log_dir)
        assert log_dir.exists()
        assert log_dir.is_dir()
        # Make sure log_dir is an empty directory
        assert not list(log_dir.iterdir())

        # Check the generated zip file
        zipFile = Path(job_path).joinpath(f"{updated_wf_commons.prod_job_id}.zip")
        assert not zipFile.exists()

        # Make sure that StorageElement was called twice (getURL, putFile)
        assert mock_se_method.call_count == 0

        # Make sure that the request was not created
        assert mock_transferAndRegisterFile.call_count == 0

        # Make sure the application status was changed
        assert mock_setApplicationStatus.call_count == 1
        assert mock_setJobParameter.call_count == 0

        shutil.rmtree(updated_wf_commons.log_dir, ignore_errors=True)

    def test_uploadLogFile_zipException(
        self, mocker: MockerFixture, uplogfile, job_path, wf_commons, prodconf_json, prodconf_py
    ):
        """Test execution of UploadLogFile module when an exception is raised when zipping files."""
        mocker.patch("LHCbDIRAC.Workflow.Modules.UploadLogFile.zipFiles", side_effect=OSError)
        mock_se_method = mocker.patch(
            "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__executeMethod",
            return_value=S_OK({"Failed": [], "Successful": {"notImportant": "notImportant"}}),
        )
        mock_transferAndRegisterFile = mocker.patch(
            "DIRAC.DataManagementSystem.Client.FailoverTransfer.FailoverTransfer.transferAndRegisterFile",
            return_value=S_OK(),
        )
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus"
        )

        WorkflowCommons(**wf_commons).save(job_path)

        uplogfile.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        # Check the log directory
        assert updated_wf_commons.log_dir != ""
        log_dir = Path(updated_wf_commons.log_dir)
        assert log_dir.exists()
        assert log_dir.is_dir()
        assert log_dir.joinpath(prodconf_json).exists()
        assert log_dir.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert log_dir.joinpath(prodconf_py).exists()
        assert log_dir.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        for file in log_dir.iterdir():
            assert file.stat().st_mode & 0o777 == 0o755

        # Check the generated zip file
        zipFile = Path(job_path).joinpath(f"{updated_wf_commons.prod_job_id}.zip")
        assert not zipFile.exists()

        # Make sure that StorageElement was called twice (getURL, putFile)
        assert mock_se_method.call_count == 0

        # Make sure that the request was not created
        assert mock_transferAndRegisterFile.call_count == 0

        # Make sure the application status was changed
        assert mock_setApplicationStatus.call_count == 1

        shutil.rmtree(updated_wf_commons.log_dir, ignore_errors=True)

    def test_uploadLogFile_zipError(
        self, mocker: MockerFixture, uplogfile, job_path, wf_commons, prodconf_json, prodconf_py
    ):
        """Test execution of UploadLogFile module when an error is occurring when zipping files."""
        mocker.patch("LHCbDIRAC.Workflow.Modules.UploadLogFile.zipFiles", return_value=S_ERROR("Error"))
        mock_se_method = mocker.patch(
            "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__executeMethod",
            return_value=S_OK({"Failed": [], "Successful": {"notImportant": "notImportant"}}),
        )
        mock_transferAndRegisterFile = mocker.patch(
            "DIRAC.DataManagementSystem.Client.FailoverTransfer.FailoverTransfer.transferAndRegisterFile",
            return_value=S_OK(),
        )
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus"
        )

        # Execute the module
        WorkflowCommons(**wf_commons).save(job_path)

        uplogfile.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        # Check the log directory
        assert updated_wf_commons.log_dir != ""
        log_dir = Path(updated_wf_commons.log_dir)
        assert log_dir.exists()
        assert log_dir.is_dir()
        assert log_dir.joinpath(prodconf_json).exists()
        assert log_dir.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert log_dir.joinpath(prodconf_py).exists()
        assert log_dir.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        for file in log_dir.iterdir():
            assert file.stat().st_mode & 0o777 == 0o755

        # Check the generated zip file
        zipFile = Path(job_path).joinpath(f"{updated_wf_commons.prod_job_id}.zip")
        assert not zipFile.exists()

        # Make sure that StorageElement was called twice (getURL, putFile)
        assert mock_se_method.call_count == 0

        # Make sure that the request was not created
        assert mock_transferAndRegisterFile.call_count == 0

        # Make sure the application status was changed
        assert mock_setApplicationStatus.call_count == 1

        shutil.rmtree(updated_wf_commons.log_dir, ignore_errors=True)

    def test_uploadLogFile_SEError(
        self, mocker: MockerFixture, uplogfile, job_path, wf_commons, prodconf_json, prodconf_py
    ):
        """Test execution of UploadLogFile module when an error is occurring when calling StorageElement."""
        mocker.patch("LHCbDIRAC.Workflow.Modules.UploadLogFile.getDestinationSEList", return_value=["SE1", "SE2"])
        mock_se_method = mocker.patch(
            "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__executeMethod",
            return_value=S_ERROR("Error"),
        )
        mock_transferAndRegisterFile = mocker.patch(
            "DIRAC.DataManagementSystem.Client.FailoverTransfer.FailoverTransfer.transferAndRegisterFile",
            return_value=S_OK({"uploadedSE": "SE1"}),
        )
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus"
        )

        WorkflowCommons(**wf_commons).save(job_path)

        uplogfile.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        # Check the log directory
        assert updated_wf_commons.log_dir != ""
        log_dir = Path(updated_wf_commons.log_dir)
        assert log_dir.exists()
        assert log_dir.is_dir()
        assert log_dir.joinpath(prodconf_json).exists()
        assert log_dir.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert log_dir.joinpath(prodconf_py).exists()
        assert log_dir.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        for file in log_dir.iterdir():
            assert file.stat().st_mode & 0o777 == 0o755

        # Check the generated zip file
        zipFile = Path(job_path).joinpath(f"{updated_wf_commons.prod_job_id}.zip")
        assert zipFile.exists()

        zipfile.ZipFile(zipFile, "r").extractall("unzipped")
        unzipped = Path("unzipped").joinpath(updated_wf_commons.prod_job_id)
        assert unzipped.joinpath(prodconf_json).exists()
        assert unzipped.joinpath(prodconf_py).exists()
        assert unzipped.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert unzipped.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        # Make sure that StorageElement was called twice (getURL, putFile)
        assert mock_se_method.call_count == 2

        # Make sure that the request was created
        assert mock_transferAndRegisterFile.call_count == 1

        operations = json.loads(updated_wf_commons.request.toJSON()["Value"])["Operations"]

        assert len(operations) == 2
        assert operations[0]["Type"] == "LogUpload"
        assert len(operations[0]["Files"]) == 1
        assert operations[0]["Files"][0]["LFN"] == updated_wf_commons.log_lfn_path

        assert operations[1]["Type"] == "RemoveFile"
        assert len(operations[1]["Files"]) == 1
        assert operations[1]["Files"][0]["LFN"] == updated_wf_commons.log_lfn_path

        # Make sure the application status was not changed
        assert mock_setApplicationStatus.call_count == 0

        shutil.rmtree(updated_wf_commons.log_dir, ignore_errors=True)

    def test_uploadLogFile_transferError(
        self, mocker: MockerFixture, uplogfile, job_path, wf_commons, prodconf_json, prodconf_py
    ):
        """Test execution of UploadLogFile module when calling StorageElement and FailoverTransfer fail."""
        mocker.patch("LHCbDIRAC.Workflow.Modules.UploadLogFile.getDestinationSEList", return_value=["SE1", "SE2"])
        mock_se_method = mocker.patch(
            "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__executeMethod",
            return_value=S_ERROR("Error"),
        )
        mock_transferAndRegisterFile = mocker.patch(
            "DIRAC.DataManagementSystem.Client.FailoverTransfer.FailoverTransfer.transferAndRegisterFile",
            return_value=S_ERROR("Error"),
        )
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus"
        )

        WorkflowCommons(**wf_commons).save(job_path)

        uplogfile.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        # Check the log directory
        assert updated_wf_commons.log_dir != ""
        log_dir = Path(updated_wf_commons.log_dir)
        assert log_dir.exists()
        assert log_dir.is_dir()
        assert log_dir.joinpath(prodconf_json).exists()
        assert log_dir.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert log_dir.joinpath(prodconf_py).exists()
        assert log_dir.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        for file in log_dir.iterdir():
            assert file.stat().st_mode & 0o777 == 0o755

        # Check the generated zip file
        zipFile = Path(job_path).joinpath(f"{updated_wf_commons.prod_job_id}.zip")
        assert zipFile.exists()

        zipfile.ZipFile(zipFile, "r").extractall("unzipped")
        unzipped = Path("unzipped").joinpath(updated_wf_commons.prod_job_id)
        assert unzipped.joinpath(prodconf_json).exists()
        assert unzipped.joinpath(prodconf_py).exists()
        assert unzipped.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert unzipped.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        # Make sure that StorageElement was called twice (getURL, putFile)
        assert mock_se_method.call_count == 2

        # Make sure that the request was not created
        assert mock_transferAndRegisterFile.call_count == 1

        operations = json.loads(updated_wf_commons.request.toJSON()["Value"])["Operations"]

        assert len(operations) == 0

        # Make sure the application status was changed
        assert mock_setApplicationStatus.call_count == 1

        shutil.rmtree(updated_wf_commons.log_dir, ignore_errors=True)
