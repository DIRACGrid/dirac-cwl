"""."""

import os
import tempfile
from urllib.parse import urljoin

import pytest
from DIRACCommon.Core.Utilities.ReturnValues import S_ERROR, S_OK
from pytest_mock import MockerFixture

from dirac_cwl_proto.commands import UploadLogFile


class TestUploadLogFile:
    """Collection of tests for the UploadLogFile command."""

    FILENAMES = ["file.txt", "file.log", "file.err", "file.out", "file.extra"]
    JOB_ID = "8042"
    PRODUCTION_ID = "95376"
    NAMESPACE = "MC"
    CONFIG_VERSION = "2016"

    @pytest.fixture
    def basedir(self):
        """Fixture to initialize the working directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            for file in self.FILENAMES:
                with open(os.path.join(tmpdir, file), "x") as f:
                    f.write("EMPTY")

            yield tmpdir

    def test_correct_file_finding(self, basedir):
        """Test output file finding."""
        files = UploadLogFile().obtain_output_files(basedir)
        files_names = [os.path.basename(file_path) for file_path in files]

        assert set(self.FILENAMES).difference(files_names) == {"file.extra"}

    def test_correct_file_extension_finding(self, basedir):
        """Test output file finding."""
        extensions = ["*.extra"]
        files = UploadLogFile().obtain_output_files(basedir, extensions)
        files_names = [os.path.basename(file_path) for file_path in files]

        assert set(self.FILENAMES).difference(files_names) == {"file.txt", "file.log", "file.err", "file.out"}

    def test_upload_ok(self, basedir, mocker: MockerFixture):
        """Test a correct upload."""
        base_lfn = f"/lhcb/{self.NAMESPACE}/{self.CONFIG_VERSION}/LOG/{self.PRODUCTION_ID.zfill(8)}/0000/"
        zip_name = self.JOB_ID.zfill(8) + ".zip"

        expected_lfn = os.path.join(base_lfn, zip_name)
        expected_path = os.path.join(basedir, zip_name)

        # Mock Operations
        mock_ops = mocker.patch("dirac_cwl_proto.commands.upload_log_file.Operations")
        mock_ops.return_value.getValue = lambda value, default=None: default

        # Mock JobReport
        mock_job_report = mocker.patch("dirac_cwl_proto.commands.upload_log_file.JobReport")
        mock_set_app_status = mocker.MagicMock()
        mock_set_job_parameter = mocker.MagicMock()
        mock_job_report.return_value.setApplicationStatus = mock_set_app_status
        mock_job_report.return_value.setJobParameter = mock_set_job_parameter

        # Mock StorageElement
        mock_se = mocker.patch("dirac_cwl_proto.commands.upload_log_file.StorageElement")
        mock_put_file = mocker.MagicMock()
        mock_get_url = mocker.MagicMock()
        mock_put_file.return_value = S_OK({"Successful": {expected_lfn: "Borked"}, "Failed": {}})
        mock_get_url.return_value = S_OK(urljoin("https://lhcb-dirac-logse.web.cern.ch/", expected_lfn))
        mock_se.return_value.putFile = mock_put_file
        mock_se.return_value.getURL = mock_get_url

        command = UploadLogFile()

        # Mock failover
        mock_failover = mocker.patch.object(command, "generate_failover_transfer")
        mock_failover.return_value = S_OK()

        result = command.execute(
            basedir,
            job_id=self.JOB_ID,
            production_id=self.PRODUCTION_ID,
            namespace=self.NAMESPACE,
            config_version=self.CONFIG_VERSION,
        )

        assert result["OK"]
        mock_get_url.assert_called_once_with(expected_path, protocol="https")
        mock_put_file.assert_called_once_with({expected_lfn: expected_path})
        mock_failover.assert_not_called()
        mock_set_app_status.assert_not_called()
        mock_set_job_parameter.assert_called_once()

    def test_upload_ok_to_failover(self, basedir, mocker: MockerFixture):
        """Test a failure to upload to the LogSE but a correct one to the Failover."""
        base_lfn = f"/lhcb/{self.NAMESPACE}/{self.CONFIG_VERSION}/LOG/{self.PRODUCTION_ID.zfill(8)}/0000/"
        zip_name = self.JOB_ID.zfill(8) + ".zip"

        expected_lfn = os.path.join(base_lfn, zip_name)
        expected_path = os.path.join(basedir, zip_name)

        # Mock Operations
        mock_ops = mocker.patch("dirac_cwl_proto.commands.upload_log_file.Operations")
        mock_ops.return_value.getValue = lambda value, default=None: default

        # Mock JobReport
        mock_job_report = mocker.patch("dirac_cwl_proto.commands.upload_log_file.JobReport")
        mock_set_app_status = mocker.MagicMock()
        mock_set_job_parameter = mocker.MagicMock()
        mock_job_report.return_value.setApplicationStatus = mock_set_app_status
        mock_job_report.return_value.setJobParameter = mock_set_job_parameter

        # Mock StorageElement
        mock_se = mocker.patch("dirac_cwl_proto.commands.upload_log_file.StorageElement")
        mock_put_file = mocker.MagicMock()
        mock_get_url = mocker.MagicMock()
        mock_put_file.return_value = S_OK({"Successful": {}, "Failed": {expected_lfn: "Borked"}})
        mock_get_url.return_value = S_OK(urljoin("https://lhcb-dirac-logse.web.cern.ch/", expected_lfn))
        mock_se.return_value.putFile = mock_put_file
        mock_se.return_value.getURL = mock_get_url

        command = UploadLogFile()

        # Mock failover
        mock_failover = mocker.patch.object(command, "generate_failover_transfer")
        mock_failover.return_value = S_OK()

        result = command.execute(
            basedir,
            job_id=self.JOB_ID,
            production_id=self.PRODUCTION_ID,
            namespace=self.NAMESPACE,
            config_version=self.CONFIG_VERSION,
        )

        assert result["OK"]
        mock_get_url.assert_called_once_with(expected_path, protocol="https")
        mock_put_file.assert_called_once_with({expected_lfn: expected_path})
        mock_failover.assert_called_once_with(expected_path, zip_name, expected_lfn)
        mock_set_app_status.assert_not_called()
        mock_set_job_parameter.assert_called_once()

    def test_upload_fail(self, basedir, mocker: MockerFixture):
        """Test both a failure to upload to the LogSE and the FailoverSE."""
        base_lfn = f"/lhcb/{self.NAMESPACE}/{self.CONFIG_VERSION}/LOG/{self.PRODUCTION_ID.zfill(8)}/0000/"
        zip_name = self.JOB_ID.zfill(8) + ".zip"

        expected_lfn = os.path.join(base_lfn, zip_name)
        expected_path = os.path.join(basedir, zip_name)

        # Mock JobReport
        mock_job_report = mocker.patch("dirac_cwl_proto.commands.upload_log_file.JobReport")
        mock_set_app_status = mocker.MagicMock()
        mock_set_job_parameter = mocker.MagicMock()
        mock_job_report.return_value.setApplicationStatus = mock_set_app_status
        mock_job_report.return_value.setJobParameter = mock_set_job_parameter

        # Mock StorageElement
        mock_se = mocker.patch("dirac_cwl_proto.commands.upload_log_file.StorageElement")
        mock_put_file = mocker.MagicMock()
        mock_get_url = mocker.MagicMock()
        mock_put_file.return_value = S_OK({"Successful": {}, "Failed": {expected_lfn: "Borked"}})
        mock_get_url.return_value = S_OK(urljoin("https://lhcb-dirac-logse.web.cern.ch/", expected_lfn))
        mock_se.return_value.putFile = mock_put_file
        mock_se.return_value.getURL = mock_get_url

        command = UploadLogFile()

        # Mock failover
        mock_failover = mocker.patch.object(command, "generate_failover_transfer")
        mock_failover.return_value = S_ERROR()

        result = command.execute(
            basedir,
            job_id=self.JOB_ID,
            production_id=self.PRODUCTION_ID,
            namespace=self.NAMESPACE,
            config_version=self.CONFIG_VERSION,
        )

        assert not result["OK"]
        mock_get_url.assert_not_called()
        mock_put_file.assert_called_once_with({expected_lfn: expected_path})
        mock_failover.assert_called_once_with(expected_path, zip_name, expected_lfn)
        mock_set_app_status.assert_called_once()
        mock_set_job_parameter.assert_not_called()

    def test_no_files_to_zip(self, basedir, mocker):
        """Test execution when the job did not return any files."""
        import shutil

        shutil.rmtree(basedir)

        # Mock JobReport
        mock_job_report = mocker.patch("dirac_cwl_proto.commands.upload_log_file.JobReport")
        mock_set_app_status = mocker.MagicMock()
        mock_set_job_parameter = mocker.MagicMock()
        mock_job_report.return_value.setApplicationStatus = mock_set_app_status
        mock_job_report.return_value.setJobParameter = mock_set_job_parameter

        result = UploadLogFile().execute(
            basedir,
            job_id=self.JOB_ID,
            production_id=self.PRODUCTION_ID,
            namespace=self.NAMESPACE,
            config_version=self.CONFIG_VERSION,
        )

        assert result["OK"]
        assert result["Value"] == "No files to upload"
        mock_set_app_status.assert_not_called()

    def test_failed_to_zip(self, basedir, mocker: MockerFixture):
        """Test failure while zipping."""
        command = UploadLogFile()

        # Mocker zip
        mock_zip = mocker.patch.object(command, "zip_files")
        mock_zip.side_effect = [AttributeError(), OSError(), ValueError()]

        # Mock JobReport
        mock_job_report = mocker.patch("dirac_cwl_proto.commands.upload_log_file.JobReport")
        mock_set_app_status = mocker.MagicMock()
        mock_set_job_parameter = mocker.MagicMock()
        mock_job_report.return_value.setApplicationStatus = mock_set_app_status
        mock_job_report.return_value.setJobParameter = mock_set_job_parameter

        # Test raising AttributeError
        result = command.execute(
            basedir,
            job_id=self.JOB_ID,
            production_id=self.PRODUCTION_ID,
            namespace=self.NAMESPACE,
            config_version=self.CONFIG_VERSION,
        )

        assert result["OK"]
        assert "Failed to zip files" in result["Value"]
        assert "AttributeError" in result["Value"]
        mock_set_app_status.assert_called_once_with("Failed to create zip of log files")
        mock_set_app_status.reset_mock()

        result = command.execute(
            basedir,
            job_id=self.JOB_ID,
            production_id=self.PRODUCTION_ID,
            namespace=self.NAMESPACE,
            config_version=self.CONFIG_VERSION,
        )

        # Test raising OSError
        assert result["OK"]
        assert "Failed to zip files" in result["Value"]
        assert "OSError" in result["Value"]
        mock_set_app_status.assert_called_once_with("Failed to create zip of log files")
        mock_set_app_status.reset_mock()

        result = command.execute(
            basedir,
            job_id=self.JOB_ID,
            production_id=self.PRODUCTION_ID,
            namespace=self.NAMESPACE,
            config_version=self.CONFIG_VERSION,
        )

        # Test raising ValueError
        assert result["OK"]
        assert "Failed to zip files" in result["Value"]
        assert "ValueError" in result["Value"]
        mock_set_app_status.assert_called_once_with("Failed to create zip of log files")

        mock_set_job_parameter.assert_not_called()
