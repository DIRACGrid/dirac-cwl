"""Tests for the commands.

This module tests the execution of the different commands.
"""

import json
import os
import shutil
import time
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from textwrap import dedent

import LHCbDIRAC
import pytest
from DIRAC import siteName
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.TransformationSystem.Client.FileReport import FileReport
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRACCommon.Core.Utilities.ReturnValues import S_ERROR, S_OK
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary
from pytest_mock import MockerFixture

from dirac_cwl.commands import (
    AnalyseXmlSummary,
    BookkeepingReport,
    FailoverRequest,
    UploadLogFile,
    UploadOutputData,
    WorkflowAccounting,
)
from dirac_cwl.core.exceptions import WorkflowProcessingException

number_of_processors = 1
job_path = "."


@pytest.fixture
def wf_commons():
    """Workflow commons dictionary fixture."""
    yield {
        "job_id": 0,
        "job_type": "merge",
        "production_id": "123",
        "prod_job_id": "00000456",
        "event_type": "123456789",
        "number_of_events": "100",
        "config_name": "aConfigName",
        "config_version": "aConfigVersion",
        "application_name": "someApp",
        "application_version": "v1r0",
        "bk_step_id": "123",
        "inputs": [],
        "outputs": [],
        "executable": "",
        "command_id": "1",
        "command_number": 1,
    }

    Path(os.path.join(job_path, "workflow_commons.json")).unlink(missing_ok=True)


@pytest.fixture
def xml_summary_file(wf_commons):
    """XMLSummaryFile file path fixture."""
    path = os.path.join(
        job_path,
        f"summary{wf_commons['application_name']}_{wf_commons['production_id']}_{wf_commons['prod_job_id']}_{wf_commons['command_id']}.xml",
    )
    yield path
    Path(path).unlink(missing_ok=True)


@pytest.fixture
def request_file(wf_commons):
    """RequstDict file path fixture."""
    path = os.path.join(job_path, f"{wf_commons['production_id']}_{wf_commons['prod_job_id']}_request.json")
    yield path
    Path(path).unlink(missing_ok=True)


def prepare_XMLSummary_file(xml_summary, content):
    """Pepares a xml summary file and returns it as a class."""
    with open(xml_summary, "w", encoding="utf-8") as f:
        f.write(content)
    return XMLSummary(xml_summary)


def get_typed_parameter_value(name, root):
    """Find the value of a specific TypedParameter by its name."""
    for child in root:
        if child.tag == "TypedParameter" and child.attrib["Name"] == name:
            return child.attrib["Value"]
    return None


def get_output_file_details(output_file):
    """Extract details from an OutputFile element."""
    details = {
        "Name": output_file.attrib["Name"],
        "TypeName": output_file.attrib["TypeName"],
        "Parameters": {},
        "Replicas": [],
    }

    for elem in output_file:
        if elem.tag == "Parameter":
            details["Parameters"][elem.attrib["Name"]] = elem.attrib["Value"]
        elif elem.tag == "Replica":
            details["Replicas"].append({"Name": elem.attrib["Name"], "Location": elem.attrib["Location"]})

    return details


def create_workflow_commons(wf_dict):
    """Dump the content of wf_commons to a file."""
    path = os.path.join(job_path, "workflow_commons.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(wf_dict, f)
    return path


class TestUploadLogFile:
    """Collection of tests for the UploadLogFile command."""

    @pytest.fixture
    def uplogfile(self, mocker, wf_commons):
        """Fixture for UploadLogFile module."""
        uplogfile = UploadLogFile()

        yield uplogfile

        Path(f"{wf_commons['prod_job_id']}.zip").unlink(missing_ok=True)
        shutil.rmtree("unzipped", ignore_errors=True)

    @pytest.fixture
    def prodconf_json(self):
        """prodconf.json file fixture."""
        filename = "prodConf_example.json"

        with open(filename, "w") as f:
            f.write('{"foo": "bar"}')

        yield filename

        Path(filename).unlink(missing_ok=True)

    @pytest.fixture
    def prodconf_py(self):
        """prodconf.py file fixture."""
        filename = "prodConf_example.py"

        with open(filename, "w") as f:
            f.write('foo = "bar"')

        yield filename

        Path(filename).unlink(missing_ok=True)

    # Test Scenarios
    def test_uploadLogFile_success(self, mocker, uplogfile, wf_commons, prodconf_json, prodconf_py):
        """Test successful execution of UploadLogFile module."""
        log_url = "notImportant"
        mockSEMethod = mocker.patch(
            "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__executeMethod",
            return_value=S_OK({"Failed": [], "Successful": {log_url: log_url}}),
        )
        mock_request = mocker.patch("dirac_cwl.commands.upload_log_file.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_log_file.FailoverTransfer")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_log_file.JobReport")

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(failover, "transferAndRegisterFile", return_value=S_OK())
        mock_failover.return_value = failover

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        mocker.patch.object(jr, "setJobParameter")
        mock_job_report.return_value = jr

        uplogfile.request = Request()

        # Execute the module
        wf_commons_path = create_workflow_commons(wf_commons)

        uplogfile.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        # Check the log directory
        assert updated_wf_commons["log_dir"] != ""
        log_dir = Path(updated_wf_commons["log_dir"])
        assert log_dir.exists()
        assert log_dir.is_dir()
        assert log_dir.joinpath(prodconf_json).exists()
        assert log_dir.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert log_dir.joinpath(prodconf_py).exists()
        assert log_dir.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        for file in log_dir.iterdir():
            assert file.stat().st_mode & 0o777 == 0o755

        # Check the generated zip file
        zipFile = Path(f"{updated_wf_commons['prod_job_id']}.zip")
        assert zipFile.exists()

        zipfile.ZipFile(zipFile, "r").extractall("unzipped")
        unzipped = Path("unzipped").joinpath(updated_wf_commons["prod_job_id"])
        assert unzipped.joinpath(prodconf_json).exists()
        assert unzipped.joinpath(prodconf_py).exists()
        assert unzipped.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert unzipped.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        # Make sure that StorageElement was called twice (getURL, putFile)
        assert mockSEMethod.call_count == 2

        # Make sure that the request was not created
        assert failover.transferAndRegisterFile.call_count == 0

        # Make sure the application status was not changed
        assert jr.setApplicationStatus.call_count == 0

        # Check the jobReport.setParameter arguments
        assert jr.setJobParameter.call_count == 1
        assert jr.setJobParameter.call_args_list
        params = jr.setJobParameter.call_args_list[0][0]
        assert params[0] == "Log URL"
        assert params[1] == f'<a href="{log_url}">Log file directory</a>'

        shutil.rmtree(updated_wf_commons["log_dir"], ignore_errors=True)

    def test_uploadLogFile_noOutputFile(self, mocker, uplogfile, wf_commons):
        """Test execution of UploadLogFile module when there is no output files.

        * populateLogDirectory should return an error, because there is no "successful" files in log_dir.
        """
        mockSEMethod = mocker.patch(
            "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__executeMethod",
            return_value=S_OK({"Failed": [], "Successful": {"notImportant": "notImportant"}}),
        )
        mock_request = mocker.patch("dirac_cwl.commands.upload_log_file.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_log_file.FailoverTransfer")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_log_file.JobReport")

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(failover, "transferAndRegisterFile", return_value=S_OK())
        mock_failover.return_value = failover

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        mocker.patch.object(jr, "setJobParameter")
        mock_job_report.return_value = jr

        # Execute the module
        wf_commons_path = create_workflow_commons(wf_commons)

        uplogfile.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        # Check the log directory
        assert updated_wf_commons["log_dir"] != ""
        log_dir = Path(updated_wf_commons["log_dir"])
        assert log_dir.exists()
        assert log_dir.is_dir()
        # Make sure log_dir is an empty directory
        assert not list(log_dir.iterdir())

        # Check the generated zip file
        zipFile = Path(f"{updated_wf_commons['prod_job_id']}.zip")
        assert not zipFile.exists()

        # Make sure that StorageElement was called twice (getURL, putFile)
        assert mockSEMethod.call_count == 0

        # Make sure that the request was not created
        assert failover.transferAndRegisterFile.call_count == 0

        # Make sure the application status was changed
        assert jr.setApplicationStatus.call_count == 1
        assert jr.setJobParameter.call_count == 0

        shutil.rmtree(updated_wf_commons["log_dir"], ignore_errors=True)

    def test_uploadLogFile_zipException(self, mocker, uplogfile, wf_commons, prodconf_json, prodconf_py):
        """Test execution of UploadLogFile module when an exception is raised when zipping files."""
        mocker.patch("LHCbDIRAC.Workflow.Modules.UploadLogFile.zipFiles", side_effect=OSError)
        mockSEMethod = mocker.patch(
            "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__executeMethod",
            return_value=S_OK({"Failed": [], "Successful": {"notImportant": "notImportant"}}),
        )
        mock_request = mocker.patch("dirac_cwl.commands.upload_log_file.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_log_file.FailoverTransfer")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_log_file.JobReport")

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(failover, "transferAndRegisterFile", return_value=S_OK())
        mock_failover.return_value = failover

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        mock_job_report.return_value = jr

        # Execute the module
        wf_commons_path = create_workflow_commons(wf_commons)

        uplogfile.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        # Check the log directory
        assert updated_wf_commons["log_dir"] != ""
        log_dir = Path(updated_wf_commons["log_dir"])
        assert log_dir.exists()
        assert log_dir.is_dir()
        assert log_dir.joinpath(prodconf_json).exists()
        assert log_dir.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert log_dir.joinpath(prodconf_py).exists()
        assert log_dir.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        for file in log_dir.iterdir():
            assert file.stat().st_mode & 0o777 == 0o755

        # Check the generated zip file
        zipFile = Path(f"{updated_wf_commons['prod_job_id']}.zip")
        assert not zipFile.exists()

        # Make sure that StorageElement was called twice (getURL, putFile)
        assert mockSEMethod.call_count == 0

        # Make sure that the request was not created
        assert failover.transferAndRegisterFile.call_count == 0

        # Make sure the application status was changed
        assert jr.setApplicationStatus.call_count == 1

        shutil.rmtree(updated_wf_commons["log_dir"], ignore_errors=True)

    def test_uploadLogFile_zipError(self, mocker, uplogfile, wf_commons, prodconf_json, prodconf_py):
        """Test execution of UploadLogFile module when an error is occurring when zipping files."""
        mocker.patch("LHCbDIRAC.Workflow.Modules.UploadLogFile.zipFiles", return_value=S_ERROR("Error"))
        mockSEMethod = mocker.patch(
            "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__executeMethod",
            return_value=S_OK({"Failed": [], "Successful": {"notImportant": "notImportant"}}),
        )
        mock_request = mocker.patch("dirac_cwl.commands.upload_log_file.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_log_file.FailoverTransfer")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_log_file.JobReport")

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(failover, "transferAndRegisterFile", return_value=S_OK())
        mock_failover.return_value = failover

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        mock_job_report.return_value = jr

        # Execute the module
        wf_commons_path = create_workflow_commons(wf_commons)

        uplogfile.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        # Check the log directory
        assert updated_wf_commons["log_dir"] != ""
        log_dir = Path(updated_wf_commons["log_dir"])
        assert log_dir.exists()
        assert log_dir.is_dir()
        assert log_dir.joinpath(prodconf_json).exists()
        assert log_dir.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert log_dir.joinpath(prodconf_py).exists()
        assert log_dir.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        for file in log_dir.iterdir():
            assert file.stat().st_mode & 0o777 == 0o755

        # Check the generated zip file
        zipFile = Path(f"{updated_wf_commons['prod_job_id']}.zip")
        assert not zipFile.exists()

        # Make sure that StorageElement was called twice (getURL, putFile)
        assert mockSEMethod.call_count == 0

        # Make sure that the request was not created
        assert failover.transferAndRegisterFile.call_count == 0

        # Make sure the application status was changed
        assert jr.setApplicationStatus.call_count == 1

        shutil.rmtree(updated_wf_commons["log_dir"], ignore_errors=True)

    def test_uploadLogFile_SEError(self, mocker, uplogfile, wf_commons, prodconf_json, prodconf_py):
        """Test execution of UploadLogFile module when an error is occurring when calling StorageElement."""
        mocker.patch("LHCbDIRAC.Workflow.Modules.UploadLogFile.getDestinationSEList", return_value=["SE1", "SE2"])
        mockSEMethod = mocker.patch(
            "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__executeMethod",
            return_value=S_ERROR("Error"),
        )
        mock_request = mocker.patch("dirac_cwl.commands.upload_log_file.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_log_file.FailoverTransfer")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_log_file.JobReport")

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(failover, "transferAndRegisterFile", return_value=S_OK({"uploadedSE": "SE1"}))
        mock_failover.return_value = failover

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        mock_job_report.return_value = jr

        # Execute the module
        wf_commons_path = create_workflow_commons(wf_commons)

        uplogfile.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        # Check the log directory
        assert updated_wf_commons["log_dir"] != ""
        log_dir = Path(updated_wf_commons["log_dir"])
        assert log_dir.exists()
        assert log_dir.is_dir()
        assert log_dir.joinpath(prodconf_json).exists()
        assert log_dir.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert log_dir.joinpath(prodconf_py).exists()
        assert log_dir.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        for file in log_dir.iterdir():
            assert file.stat().st_mode & 0o777 == 0o755

        # Check the generated zip file
        zipFile = Path(f"{updated_wf_commons['prod_job_id']}.zip")
        assert zipFile.exists()

        zipfile.ZipFile(zipFile, "r").extractall("unzipped")
        unzipped = Path("unzipped").joinpath(updated_wf_commons["prod_job_id"])
        assert unzipped.joinpath(prodconf_json).exists()
        assert unzipped.joinpath(prodconf_py).exists()
        assert unzipped.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert unzipped.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        # Make sure that StorageElement was called twice (getURL, putFile)
        assert mockSEMethod.call_count == 2

        # Make sure that the request was created
        assert failover.transferAndRegisterFile.call_count == 1

        operations = updated_wf_commons["request_dict"]["Operations"]

        assert len(operations) == 2
        assert operations[0]["Type"] == "LogUpload"
        assert len(operations[0]["Files"]) == 1
        assert operations[0]["Files"][0]["LFN"] == updated_wf_commons["log_lfn_path"]

        assert operations[1]["Type"] == "RemoveFile"
        assert len(operations[1]["Files"]) == 1
        assert operations[1]["Files"][0]["LFN"] == updated_wf_commons["log_lfn_path"]

        # Make sure the application status was not changed
        assert jr.setApplicationStatus.call_count == 0

        shutil.rmtree(updated_wf_commons["log_dir"], ignore_errors=True)

    def test_uploadLogFile_transferError(self, mocker, uplogfile, wf_commons, prodconf_json, prodconf_py):
        """Test execution of UploadLogFile module when calling StorageElement and FailoverTransfer fail."""
        mocker.patch("LHCbDIRAC.Workflow.Modules.UploadLogFile.getDestinationSEList", return_value=["SE1", "SE2"])
        mockSEMethod = mocker.patch(
            "DIRAC.Resources.Storage.StorageElement.StorageElementItem._StorageElementItem__executeMethod",
            return_value=S_ERROR("Error"),
        )
        mock_request = mocker.patch("dirac_cwl.commands.upload_log_file.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_log_file.FailoverTransfer")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_log_file.JobReport")

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(failover, "transferAndRegisterFile", return_value=S_ERROR("Error"))
        mock_failover.return_value = failover

        mock_job_report = mocker.patch("dirac_cwl.commands.upload_log_file.JobReport")
        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        mock_job_report.return_value = jr

        # Execute the module
        wf_commons_path = create_workflow_commons(wf_commons)

        uplogfile.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        # Check the log directory
        assert updated_wf_commons["log_dir"] != ""
        log_dir = Path(updated_wf_commons["log_dir"])
        assert log_dir.exists()
        assert log_dir.is_dir()
        assert log_dir.joinpath(prodconf_json).exists()
        assert log_dir.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert log_dir.joinpath(prodconf_py).exists()
        assert log_dir.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        for file in log_dir.iterdir():
            assert file.stat().st_mode & 0o777 == 0o755

        # Check the generated zip file
        zipFile = Path(f"{updated_wf_commons['prod_job_id']}.zip")
        assert zipFile.exists()

        zipfile.ZipFile(zipFile, "r").extractall("unzipped")
        unzipped = Path("unzipped").joinpath(updated_wf_commons["prod_job_id"])
        assert unzipped.joinpath(prodconf_json).exists()
        assert unzipped.joinpath(prodconf_py).exists()
        assert unzipped.joinpath(prodconf_json).read_text() == '{"foo": "bar"}'
        assert unzipped.joinpath(prodconf_py).read_text() == 'foo = "bar"'

        # Make sure that StorageElement was called twice (getURL, putFile)
        assert mockSEMethod.call_count == 2

        # Make sure that the request was not created
        assert failover.transferAndRegisterFile.call_count == 1

        operations = updated_wf_commons["request_dict"]["Operations"]

        assert len(operations) == 0

        # Make sure the application status was changed
        assert jr.setApplicationStatus.call_count == 1

        shutil.rmtree(updated_wf_commons["log_dir"], ignore_errors=True)


class TestBookkeepingReport:
    """Collection of tests for the BookkeepingReport command."""

    @pytest.fixture
    def bookkeeping_file(self, wf_commons):
        """Bookkeeping report file fixture."""
        path = os.path.join(job_path, f"bookkeeping_{wf_commons['command_id']}.xml")
        yield path
        Path(path).unlink(missing_ok=True)

    @pytest.fixture
    def bk_report(self, mocker):
        """BookkeepingReport mocked command.

        Cleans created files after execution.
        """
        mock_get_n_procs = mocker.patch("dirac_cwl.commands.bookkeeping_report.getNumberOfProcessorsToUse")

        mock_get_n_procs.return_value = number_of_processors

        yield BookkeepingReport()

        Path("00209455_00001537_1").unlink(missing_ok=True)
        Path("00209455_00001537_1.sim").unlink(missing_ok=True)
        Path("application.log").unlink(missing_ok=True)

    def test_bkreport_prod_mcsimulation_success(self, bk_report, wf_commons, bookkeeping_file, xml_summary_file):
        """Test successful execution of BookkeepingReport module."""
        wf_commons["application_name"] = "Gauss"
        wf_commons["job_type"] = "MCSimulation"

        wf_commons["bookkeeping_LFNs"] = [
            "/lhcb/LHCb/Collision16/SIM/00209455/0000/00209455_00001537_1.sim",
        ]
        wf_commons["production_output_data"] = [
            "/lhcb/LHCb/Collision16/SIM/00209455/0000/00209455_00001537_1.sim",
        ]

        wf_commons["start_time"] = time.time() - 1000

        # Input data should be None as we use Gauss (MCSimulation)
        wf_commons["outputs"] = [
            {"outputDataName": "00209455_00001537_1.sim", "outputDataType": "sim"},
        ]
        Path(wf_commons["outputs"][0]["outputDataName"]).touch()

        # Mock the XMLSummary object
        xml_content = dedent("""\
            <?xml version="1.0" encoding="UTF-8"?>
            <summary xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"
            xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd">
                <success>True</success>
                <step>finalize</step>
                <usage>
                    <stat useOf="MemoryMaximum" unit="KB">2129228.0</stat>
                </usage>
                <input />
                <output>
                    <file GUID="F2A331E0-C977-11EE-8689-D85ED3091B7C" name="PFN:00209455_00001537_1.sim" status="full">
                    1
                    </file>
                </output>
                <counters>
                    <counter name="ConversionFilter/event with gamma conversion from">1</counter>
                    <counter name="GaussGeo.Hcal/#energy">77</counter>
                    <counter name="GaussGeo.Hcal/#hits">2644</counter>
                    <counter name="GaussGeo.Hcal/#subhits">6262</counter>
                    <counter name="GaussGeo.Hcal/#tslots">8391</counter>
                    <counter name="GaussGeo.Ecal/#energy">963</counter>
                    <counter name="GaussGeo.Ecal/#hits">18139</counter>
                    <counter name="GaussGeo.Ecal/#subhits">45169</counter>
                    <counter name="GaussGeo.Ecal/#tslots">52237</counter>
                    <counter name="CounterSummarySvc/handled">79</counter>
                </counters>
                <lumiCounters />
            </summary>
            """)

        wf_commons["xml_summary_path"] = xml_summary_file
        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute the module
        bk_report.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        xml_path = bookkeeping_file
        assert Path(xml_path).exists(), "XML report file not created."

        # Validate the XML file
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Extract fields from the XML and perform further operations
        assert root.tag == "Job", "Root tag should be Job."
        assert root.attrib["ConfigName"] == updated_wf_commons["config_name"]
        assert root.attrib["ConfigVersion"] == updated_wf_commons["config_version"]
        assert root.attrib["Date"]
        assert root.attrib["Time"]

        assert get_typed_parameter_value("ProgramName", root) == updated_wf_commons["application_name"]
        assert get_typed_parameter_value("ProgramVersion", root) == updated_wf_commons["application_version"]
        assert get_typed_parameter_value("DiracVersion", root) == LHCbDIRAC.__version__
        assert get_typed_parameter_value("Name", root) == updated_wf_commons["command_id"]
        assert float(get_typed_parameter_value("ExecTime", root)) > 1000
        assert get_typed_parameter_value("CPUTIME", root) == "0"

        assert get_typed_parameter_value("FirstEventNumber", root) == "1"
        assert get_typed_parameter_value("StatisticsRequested", root) == str(updated_wf_commons["number_of_events"])
        assert get_typed_parameter_value("NumberOfEvents", root) == str(xf_o.outputEventsTotal)

        assert get_typed_parameter_value("Production", root) == updated_wf_commons["production_id"]
        assert get_typed_parameter_value("DiracJobId", root) == str(updated_wf_commons["job_id"])
        assert get_typed_parameter_value("Location", root) == siteName()
        assert get_typed_parameter_value("JobStart", root)
        assert get_typed_parameter_value("JobEnd", root)
        assert get_typed_parameter_value("JobType", root) == updated_wf_commons["job_type"]

        assert get_typed_parameter_value("WorkerNode", root)
        assert get_typed_parameter_value("WNMEMORY", root)
        assert get_typed_parameter_value("WNCPUPOWER", root)
        assert get_typed_parameter_value("WNMODEL", root)
        assert get_typed_parameter_value("WNCACHE", root)
        assert get_typed_parameter_value("WNCPUHS06", root)
        assert get_typed_parameter_value("NumberOfProcessors", root) == str(number_of_processors)

        # Input should be empty
        input_file = root.find("InputFile")
        assert input_file is None, "InputFile element should not be present."

        # Output should not be empty
        output_files = root.findall("OutputFile")
        assert output_files, "No OutputFile elements found."

        first_output_details = get_output_file_details(output_files[0])
        assert first_output_details["Name"] == updated_wf_commons["production_output_data"][0]
        assert first_output_details["TypeName"] == "SIM"
        assert first_output_details["Parameters"]["FileSize"] == "0"
        assert "CreationDate" in first_output_details["Parameters"]
        assert "MD5Sum" in first_output_details["Parameters"]
        assert "Guid" in first_output_details["Parameters"]

        assert len(output_files) == 1

    def test_bkreport_prod_mcsimulation_noinputoutput_success(
        self, bk_report, wf_commons, bookkeeping_file, xml_summary_file
    ):
        """Test successful execution of BookkeepingReport module.

        * No input files because wf_commons["stepInputData is empty
        * No output files because wf_commons["stepOutputData is empty
        * No pool xml catalog
        * Simulation conditions because the application used is Gauss
        """
        # Mock the BookkeepingReport module
        wf_commons["application_name"] = "Gauss"
        wf_commons["job_type"] = "MCSimulation"

        # This was obtained from a previous module (likely GaudiApplication)
        wf_commons["bookkeeping_LFNs"] = [
            "/lhcb/LHCb/Collision16/SIM/00209455/0000/00209455_00001537_1",
        ]
        wf_commons["production_output_data"] = [
            "/lhcb/LHCb/Collision16/SIM/00209455/0000/00209455_00001537_1",
        ]

        wf_commons["start_time"] = time.time() - 1000

        # Mock the XMLSummary object
        xml_content = dedent("""\
            <?xml version="1.0" encoding="UTF-8"?>
            <summary xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.0"
            xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd">
                <success>True</success>
                <step>finalize</step>
                <usage>
                    <stat useOf="MemoryMaximum" unit="KB">2129228.0</stat>
                </usage>
                <input />
                <output>
                    <file GUID="F2A331E0-C977-11EE-8689-D85ED3091B7C" name="PFN:00211518_00024143_1.sim" status="full">
                    1
                    </file>
                </output>
                <counters>
                    <counter name="ConversionFilter/event with gamma conversion from">1</counter>
                    <counter name="GaussGeo.Hcal/#energy">77</counter>
                    <counter name="GaussGeo.Hcal/#hits">2644</counter>
                    <counter name="GaussGeo.Hcal/#subhits">6262</counter>
                    <counter name="GaussGeo.Hcal/#tslots">8391</counter>
                    <counter name="GaussGeo.Ecal/#energy">963</counter>
                    <counter name="GaussGeo.Ecal/#hits">18139</counter>
                    <counter name="GaussGeo.Ecal/#subhits">45169</counter>
                    <counter name="GaussGeo.Ecal/#tslots">52237</counter>
                    <counter name="CounterSummarySvc/handled">79</counter>
                </counters>
                <lumiCounters />
            </summary>
            """)

        wf_commons["xml_summary_path"] = xml_summary_file
        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute the module
        bk_report.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        # Check if the XML report file is created
        xml_path = bookkeeping_file
        assert Path(xml_path).exists(), "XML report file not created."

        # Validate the XML file
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Extract fields from the XML and perform further operations
        assert root.tag == "Job", "Root tag should be Job."
        assert root.attrib["ConfigName"] == updated_wf_commons["config_name"]
        assert root.attrib["ConfigVersion"] == updated_wf_commons["config_version"]
        assert root.attrib["Date"]
        assert root.attrib["Time"]

        assert get_typed_parameter_value("ProgramName", root) == updated_wf_commons["application_name"]
        assert get_typed_parameter_value("ProgramVersion", root) == updated_wf_commons["application_version"]
        assert get_typed_parameter_value("DiracVersion", root) == LHCbDIRAC.__version__
        assert get_typed_parameter_value("Name", root) == updated_wf_commons["command_id"]
        assert float(get_typed_parameter_value("ExecTime", root)) > 1000
        assert get_typed_parameter_value("CPUTIME", root) == "0"

        assert get_typed_parameter_value("FirstEventNumber", root) == "1"
        assert get_typed_parameter_value("StatisticsRequested", root) == str(updated_wf_commons["number_of_events"])
        assert get_typed_parameter_value("NumberOfEvents", root) == str(xf_o.outputEventsTotal)

        assert get_typed_parameter_value("Production", root) == updated_wf_commons["production_id"]
        assert get_typed_parameter_value("DiracJobId", root) == str(updated_wf_commons["job_id"])
        assert get_typed_parameter_value("Location", root) == siteName()
        assert get_typed_parameter_value("JobStart", root)
        assert get_typed_parameter_value("JobEnd", root)
        assert get_typed_parameter_value("JobType", root) == updated_wf_commons["job_type"]

        assert get_typed_parameter_value("WorkerNode", root)
        assert get_typed_parameter_value("WNMEMORY", root)
        assert get_typed_parameter_value("WNCPUPOWER", root)
        assert get_typed_parameter_value("WNMODEL", root)
        assert get_typed_parameter_value("WNCACHE", root)
        assert get_typed_parameter_value("WNCPUHS06", root)
        assert get_typed_parameter_value("NumberOfProcessors", root) == str(number_of_processors)

        # Input should be empty
        input_file = root.find("InputFile")
        assert input_file is None, "InputFile element should not be present."

        # Output should be empty
        output_file = root.find("OutputFile")
        assert output_file is None, "OutputFile element should not be present."

    def test_bk_report_prod_mcreconstruction_success(self, bk_report, wf_commons, bookkeeping_file, xml_summary_file):
        """Test successful execution of BookkeepingReport module."""
        wf_commons["application_name"] = "Boole"
        wf_commons["job_type"] = "MCReconstruction"

        wf_commons["bookkeeping_LFNs"] = [
            "/lhcb/LHCb/Collision16/SIM/00209455/0000/00209455_00001537_1",
        ]
        wf_commons["log_file_path"] = "/lhcb/LHCb/Collision16/LOG/00209455/0000/"
        wf_commons["production_output_data"] = [
            "/lhcb/LHCb/Collision16/SIM/00209455/0000/00209455_00001537_1",
        ]

        wf_commons["start_time"] = time.time() - 1000

        wf_commons["inputs"] = ["/lhcb/MC/2018/SIM/00212581/0000/00212581_00001446_1.sim"]
        wf_commons["outputs"] = [
            {"outputDataName": "00209455_00001537_1", "outputDataType": "digi"},
        ]
        wf_commons["application_log"] = "application.log"
        Path(wf_commons["application_log"]).touch()
        Path(wf_commons["outputs"][0]["outputDataName"]).touch()

        # Mock the XMLSummary object
        xml_content = dedent("""\
            <?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:0209455_00001537_1.sim"
                            status="full">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        wf_commons["xml_summary_path"] = xml_summary_file

        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute the module
        bk_report.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        # Check if the XML report file is created
        xml_path = bookkeeping_file
        assert Path(xml_path).exists(), "XML report file not created."

        # Validate the XML file
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Extract fields from the XML and perform further operations
        assert root.tag == "Job", "Root tag should be Job."
        assert root.attrib["ConfigName"] == updated_wf_commons["config_name"]
        assert root.attrib["ConfigVersion"] == updated_wf_commons["config_version"]
        assert root.attrib["Date"]
        assert root.attrib["Time"]

        assert get_typed_parameter_value("ProgramName", root) == updated_wf_commons["application_name"]
        assert get_typed_parameter_value("ProgramVersion", root) == updated_wf_commons["application_version"]
        assert get_typed_parameter_value("DiracVersion", root) == LHCbDIRAC.__version__
        assert get_typed_parameter_value("Name", root) == updated_wf_commons["command_id"]
        assert float(get_typed_parameter_value("ExecTime", root)) > 1000
        assert get_typed_parameter_value("CPUTIME", root) == "0"

        assert get_typed_parameter_value("FirstEventNumber", root) == "1"
        assert get_typed_parameter_value("StatisticsRequested", root) == str(updated_wf_commons["number_of_events"])
        assert get_typed_parameter_value("NumberOfEvents", root) == str(xf_o.inputEventsTotal)

        assert get_typed_parameter_value("Production", root) == updated_wf_commons["production_id"]
        assert get_typed_parameter_value("DiracJobId", root) == str(updated_wf_commons["job_id"])
        assert get_typed_parameter_value("Location", root) == siteName()
        assert get_typed_parameter_value("JobStart", root)
        assert get_typed_parameter_value("JobEnd", root)
        assert get_typed_parameter_value("JobType", root) == updated_wf_commons["job_type"]

        assert get_typed_parameter_value("WorkerNode", root)
        assert get_typed_parameter_value("WNMEMORY", root)
        assert get_typed_parameter_value("WNCPUPOWER", root)
        assert get_typed_parameter_value("WNMODEL", root)
        assert get_typed_parameter_value("WNCACHE", root)
        assert get_typed_parameter_value("WNCPUHS06", root)
        assert get_typed_parameter_value("NumberOfProcessors", root) == str(number_of_processors)

        # Input should not be empty
        input_file = root.find("InputFile")
        assert input_file is not None, "InputFile element should be present."

        # Output should not be empty
        output_files = root.findall("OutputFile")
        assert output_files, "No OutputFile elements found."

        first_output_details = get_output_file_details(output_files[0])
        assert first_output_details["Name"] == updated_wf_commons["production_output_data"][0]
        assert first_output_details["TypeName"] == "DIGI"
        assert first_output_details["Parameters"]["FileSize"] == "0"
        assert "CreationDate" in first_output_details["Parameters"]
        assert "MD5Sum" in first_output_details["Parameters"]
        assert "Guid" in first_output_details["Parameters"]

        assert len(output_files) == 1

        # Because we are using Gauss, sim conditions should be present too
        simulation_condition = root.find("SimulationCondition")
        assert simulation_condition is None, "SimulationCondition element should not be present."

    def test_bkreport_previousError_success(self, mocker, bk_report, wf_commons, bookkeeping_file):
        """Test previous command failure."""
        wf_commons["application_name"] = "Gauss"
        wf_commons["application_version"] = wf_commons["config_version"]
        wf_commons["job_type"] = "MCSimulation"
        wf_commons["step_status"] = S_ERROR()

        create_workflow_commons(wf_commons)

        bk_report.execute(job_path)

        assert not os.path.exists(bookkeeping_file)


class TestFailoverRequest:
    """Collection of tests for the FailoverRequest command."""

    @pytest.fixture
    def failover_request(self, mocker: MockerFixture):
        """FailoverRequest mocked command.

        Cleans created files after execution.
        """
        mocker.patch("dirac_cwl.commands.failover_request.RequestValidator")

        yield FailoverRequest()

    def test_failoverRequest_success(self, mocker: MockerFixture, failover_request, wf_commons, request_file):
        """Test successful execution of FailoverRequest module."""
        problematic_files = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000287_1.ew.dst",
        ]

        mock_file_report = mocker.patch("dirac_cwl.commands.failover_request.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.failover_request.JobReport")

        fr = FileReport()
        mocker.patch.object(fr, "getFiles", side_effect=[problematic_files, []])
        mocker.patch.object(fr, "commit", return_value=S_OK("Anything"))
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        mock_job_report.return_value = jr

        wf_commons["inputs"] = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
            "/lhcb/data/2011/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
        ] + problematic_files

        wf_commons_path = create_workflow_commons(wf_commons)

        failover_request.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        # Check the FileReport calls: the problematic file should not appear
        # The input files should be set to "Processed"
        assert fr.setFileStatus.call_count == 2
        args = fr.setFileStatus.call_args_list
        assert args[0][0][0] == int(updated_wf_commons["production_id"])
        assert args[0][0][1] == updated_wf_commons["inputs"][0]
        assert args[0][0][2] == "Processed"

        assert args[1][0][0] == int(updated_wf_commons["production_id"])
        assert args[1][0][1] == updated_wf_commons["inputs"][1]
        assert args[1][0][2] == "Processed"

        # Make sure the appliction is successfully finished
        assert jr.setApplicationStatus.call_count == 1
        assert jr.setApplicationStatus.call_args[0][0] == "Job Finished Successfully"

        # Make sure the forward DISET is not generated
        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 0

        # Make sure the request json does not exists
        assert not Path(request_file).exists()

    def test_failoverRequest_commitFailure1(self, mocker: MockerFixture, failover_request, wf_commons, request_file):
        """Test execution of FailoverRequest module when the fileReport.commit() fails.

        In this context, the second call to commit() will work, so the request should not be generated.
        """
        problematic_files = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000287_1.ew.dst",
        ]
        # Both calla to getFiles() will return the problematic files because the commit did not work
        mock_file_report = mocker.patch("dirac_cwl.commands.failover_request.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.failover_request.JobReport")

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        mock_job_report.return_value = jr

        fr = FileReport()
        mocker.patch.object(fr, "getFiles", side_effect=[problematic_files, problematic_files])
        mocker.patch.object(fr, "commit", side_effect=[S_ERROR("Error"), S_OK(None)])
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        wf_commons["inputs"] = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
            "/lhcb/data/2011/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
        ] + problematic_files

        # Execute the module
        wf_commons_path = create_workflow_commons(wf_commons)

        failover_request.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        # Check the FileReport calls: the problematic file should not appear
        # The input files should be set to "Processed"
        assert fr.setFileStatus.call_count == 2
        args = fr.setFileStatus.call_args_list
        assert args[0][0][0] == int(updated_wf_commons["production_id"])
        assert args[0][0][1] == updated_wf_commons["inputs"][0]
        assert args[0][0][2] == "Processed"

        assert args[1][0][0] == int(updated_wf_commons["production_id"])
        assert args[1][0][1] == updated_wf_commons["inputs"][1]
        assert args[1][0][2] == "Processed"

        # Make sure the appliction is successfully finished
        assert jr.setApplicationStatus.call_count == 1
        assert jr.setApplicationStatus.call_args[0][0] == "Job Finished Successfully"

        # Make sure the forward DISET is generated
        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 0

        # Make sure the request json does not exists
        assert not Path(request_file).exists()

    def test_failoverRequest_commitFailure2(self, mocker: MockerFixture, failover_request, wf_commons, request_file):
        """Test execution of FailoverRequest module when the fileReport.commit() fails.

        In this context, the second call to commit() will fail, so the request should be generated.
        """
        problematic_files = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000287_1.ew.dst",
        ]
        # Both calla to getFiles() will return the problematic files because the commit did not work
        mock_file_report = mocker.patch("dirac_cwl.commands.failover_request.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.failover_request.JobReport")

        fr = FileReport()
        mocker.patch.object(fr, "getFiles", side_effect=[problematic_files, problematic_files])
        mocker.patch.object(fr, "commit", side_effect=[S_ERROR("Error"), S_ERROR("Error")])
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        mock_job_report.return_value = jr

        wf_commons["inputs"] = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
            "/lhcb/data/2011/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
        ] + problematic_files

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute the module
        failover_request.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        # Check the FileReport calls: the problematic file should not appear
        # The input files should be set to "Processed"
        assert fr.setFileStatus.call_count == 2
        args = fr.setFileStatus.call_args_list
        assert args[0][0][0] == int(updated_wf_commons["production_id"])
        assert args[0][0][1] == updated_wf_commons["inputs"][0]
        assert args[0][0][2] == "Processed"

        assert args[1][0][0] == int(updated_wf_commons["production_id"])
        assert args[1][0][1] == updated_wf_commons["inputs"][1]
        assert args[1][0][2] == "Processed"

        # Make sure the appliction is successfully finished
        assert jr.setApplicationStatus.call_count == 1
        assert jr.setApplicationStatus.call_args[0][0] == "Job Finished Successfully"

        # Make sure the forward DISET is generated
        operations = updated_wf_commons["request_dict"]["Operations"]

        assert len(operations) == 1
        assert operations[0]["Type"] == "SetFileStatus"

        # Make sure the request json does not exists
        assert Path(request_file).exists()

    def test_failoverRequest_previousError_fail(
        self, mocker: MockerFixture, failover_request, wf_commons, request_file
    ):
        """Test FailoverRequest with an intentional failure."""
        problematic_files = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000287_1.ew.dst",
        ]
        mock_file_report = mocker.patch("dirac_cwl.commands.failover_request.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.failover_request.JobReport")

        fr = FileReport()
        mocker.patch.object(fr, "getFiles", side_effect=[problematic_files, problematic_files])
        mocker.patch.object(fr, "commit", side_effect=[S_ERROR("Error"), S_ERROR("Error")])
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        mock_job_report.return_value = jr

        wf_commons["inputs"] = [
            "/lhcb/data/2010/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
            "/lhcb/data/2011/EW.DST/00008380/0000/00008380_00000281_1.ew.dst",
        ] + problematic_files

        # Intentional error
        wf_commons["step_status"] = S_ERROR()

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute the module
        failover_request.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        # Check the FileReport calls: the problematic file should not appear
        # The input files should be set to "Unused"
        assert fr.setFileStatus.call_count == 2
        args = fr.setFileStatus.call_args_list
        assert args[0][0][0] == int(updated_wf_commons["production_id"])
        assert args[0][0][1] == updated_wf_commons["inputs"][0]
        assert args[0][0][2] == "Unused"

        assert args[1][0][0] == int(updated_wf_commons["production_id"])
        assert args[1][0][1] == updated_wf_commons["inputs"][1]
        assert args[1][0][2] == "Unused"

        # Make sure the appliction is not reported as a success
        assert jr.setApplicationStatus.call_count == 0

        # Make sure the forward DISET is not generated
        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 0

        # Make sure the request json does not exists
        assert not Path(request_file).exists()


class TestUploadOutputDataFile:
    """Collection of tests for the UploadOutputData command."""

    OUTPUT_DATA_STEP = "1"

    @pytest.fixture
    def sim_file(self, wf_commons):
        """Sim result file fixture."""
        path = f"{wf_commons['production_id']}_{wf_commons['prod_job_id']}_{self.OUTPUT_DATA_STEP}.sim"
        with open(path, "w") as f:
            f.write("Bookkeeping file content")
        yield path
        Path(path).unlink(missing_ok=True)

    @pytest.fixture
    def bk_file(self, wf_commons):
        """Bookkeeping file fixture."""
        path = os.path.join(job_path, f"bookkeeping_{wf_commons['production_id']}_{wf_commons['prod_job_id']}.xml")
        with open(path, "w") as f:
            f.write("Sim file content")
        yield path
        Path(path).unlink(missing_ok=True)

    @pytest.fixture
    def watchdog_file(self, wf_commons):
        """Watchdog file fixture."""
        path = os.path.join(job_path, "DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK")
        yield path
        Path(path).unlink(missing_ok=True)

    @pytest.fixture
    def upload_output(self, mocker, wf_commons):
        """Fixture for UploadOutputData module."""
        mocker.patch("dirac_cwl.commands.upload_output_data.getDestinationSEList", return_value=["CERN", "CNAF"])
        mocker.patch("LHCbDIRAC.Workflow.Modules.UploadOutputData.getDestinationSEList", return_value=["CERN", "CNAF"])

        # Mock FileCatalog
        mocker.patch("DIRAC.Resources.Catalog.FileCatalog.FileCatalog.__init__", return_value=None)
        mocker.patch("DIRAC.Resources.Catalog.FileCatalog.FileCatalog.__getattr__", return_value=lambda x: S_OK({}))

        if "ProductionOutputData" in wf_commons:
            wf_commons.pop("ProductionOutputData")

        yield UploadOutputData()

    # Test Scenarios
    def test_uploadOutputData_success(self, mocker, upload_output, wf_commons, sim_file, bk_file):
        """Test successful execution of UploadOutputData module.

        * The output should be uploaded and registered in the bookkeeping system.
        * The bookkeeping report should be sent and the job parameter updated.
        """
        mock_file_report = mocker.patch("dirac_cwl.commands.upload_output_data.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_output_data.JobReport")
        mock_request = mocker.patch("dirac_cwl.commands.upload_output_data.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_output_data.FailoverTransfer")
        mock_bk_client = mocker.patch("dirac_cwl.commands.upload_output_data.BookkeepingClient")

        fr = FileReport()
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setJobParameter")
        mock_job_report.return_value = jr

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(
            failover, "transferAndRegisterFile", return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file})
        )
        mocker.patch.object(failover, "transferAndRegisterFileFailover")
        mock_failover.return_value = failover

        bkClient = BookkeepingClient()
        mocker.patch.object(bkClient, "sendXMLBookkeepingReport", return_value=S_OK())
        mock_bk_client.return_value = bkClient

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": "Tier1-Buffer",
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute module
        upload_output.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        assert fr.setFileStatus.call_count == 0
        assert bkClient.sendXMLBookkeepingReport.call_count == 1

        assert failover.transferAndRegisterFile.call_count == 1
        assert failover.transferAndRegisterFile.call_args[1]["fileName"] == sim_file

        assert failover.transferAndRegisterFileFailover.call_count == 0

        assert jr.setJobParameter.call_count == 1
        assert jr.setJobParameter.call_args[0][0] == "UploadedOutputData"
        assert jr.setJobParameter.call_args[0][1] == sim_file

        # Make sure the forward DISET is not generated
        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 0

    def test_uploadOutputData_failedBKRegistration(self, mocker, upload_output, wf_commons, sim_file, bk_file):
        """Test execution of UploadOutputData module when the BK registation fails.

        * The output should be uploaded but not registered in the bookkeeping system now.
        """
        mock_file_report = mocker.patch("dirac_cwl.commands.upload_output_data.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_output_data.JobReport")
        mock_request = mocker.patch("dirac_cwl.commands.upload_output_data.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_output_data.FailoverTransfer")
        mock_bk_client = mocker.patch("dirac_cwl.commands.upload_output_data.BookkeepingClient")

        fr = FileReport()
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setJobParameter")
        mock_job_report.return_value = jr

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(
            failover, "transferAndRegisterFile", return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file})
        )
        mocker.patch.object(failover, "transferAndRegisterFileFailover")
        mock_failover.return_value = failover

        bkClient = BookkeepingClient()
        mocker.patch.object(bkClient, "sendXMLBookkeepingReport", return_value=S_OK())
        mock_bk_client.return_value = bkClient

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
            "SIM": "Tier1-Buffer",
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute module
        upload_output.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        assert fr.setFileStatus.call_count == 0
        assert bkClient.sendXMLBookkeepingReport.call_count == 1

        assert failover.transferAndRegisterFile.call_count == 1
        assert failover.transferAndRegisterFile.call_args[1]["fileName"] == sim_file

        assert failover.transferAndRegisterFileFailover.call_count == 0

        assert jr.setJobParameter.call_count == 1
        assert jr.setJobParameter.call_args[0][0] == "UploadedOutputData"
        assert jr.setJobParameter.call_args[0][1] == sim_file

        # Make sure the request is generated
        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 1

        assert operations[0]["Type"] == "RegisterFile"
        assert operations[0]["Catalog"] == "BookkeepingDB"
        assert sim_file in operations[0]["Files"][0]["LFN"]

    def test_uploadOutputData_postponeBKRegistration(self, mocker, upload_output, wf_commons, sim_file, bk_file):
        """Test execution of UploadOutputData module when there is already a RegisterFile operation on the output.

        * The output should be uploaded but not registered in the bookkeeping system now.
        """
        mock_file_report = mocker.patch("dirac_cwl.commands.upload_output_data.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_output_data.JobReport")
        mock_request = mocker.patch("dirac_cwl.commands.upload_output_data.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_output_data.FailoverTransfer")
        mock_bk_client = mocker.patch("dirac_cwl.commands.upload_output_data.BookkeepingClient")

        fr = FileReport()
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setJobParameter")
        mock_job_report.return_value = jr

        # Mock a previous failover request: the BK registration should be postponed and added to the request
        req = Request()
        file1 = File()
        file1.LFN = (
            f"/lhcb/{wf_commons['config_name']}/{wf_commons['config_version']}"
            f"/SIM/00000{wf_commons['production_id']}/0000/{sim_file}"
        )
        o1 = Operation()
        o1.Type = "RegisterFile"
        o1.addFile(file1)
        req.addOperation(o1)
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(
            failover, "transferAndRegisterFile", return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file})
        )
        mocker.patch.object(failover, "transferAndRegisterFileFailover")
        mock_failover.return_value = failover

        bkClient = BookkeepingClient()
        mocker.patch.object(bkClient, "sendXMLBookkeepingReport", return_value=S_OK())
        mock_bk_client.return_value = bkClient

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": "Tier1-Buffer",
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute module
        upload_output.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        assert fr.setFileStatus.call_count == 0
        assert bkClient.sendXMLBookkeepingReport.call_count == 1

        assert failover.transferAndRegisterFile.call_count == 1
        assert failover.transferAndRegisterFile.call_args[1]["fileName"] == sim_file

        assert failover.transferAndRegisterFileFailover.call_count == 0

        assert jr.setJobParameter.call_count == 1
        assert jr.setJobParameter.call_args[0][0] == "UploadedOutputData"
        assert jr.setJobParameter.call_args[0][1] == sim_file

        # Make sure the request is generated
        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 2

        assert operations[0]["Type"] == "RegisterFile"
        assert operations[0]["Catalog"] is None
        assert sim_file in operations[0]["Files"][0]["LFN"]

        assert operations[1]["Type"] == "RegisterFile"
        assert operations[1]["Catalog"] == "BookkeepingDB"
        assert sim_file in operations[1]["Files"][0]["LFN"]

    def test_uploadOutputData_errorBKRegistration(self, mocker, upload_output, wf_commons, sim_file, bk_file):
        """Test execution of UploadOutputData module when an error occurs during the BK registation.

        * The output should be uploaded but not registered in the bookkeeping system at all.
        """
        mock_file_report = mocker.patch("dirac_cwl.commands.upload_output_data.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_output_data.JobReport")
        mock_request = mocker.patch("dirac_cwl.commands.upload_output_data.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_output_data.FailoverTransfer")
        mock_bk_client = mocker.patch("dirac_cwl.commands.upload_output_data.BookkeepingClient")

        fr = FileReport()
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setJobParameter")
        mock_job_report.return_value = jr

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(
            failover, "transferAndRegisterFile", return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file})
        )
        mocker.patch.object(failover, "transferAndRegisterFileFailover")
        mock_failover.return_value = failover

        bkClient = BookkeepingClient()
        mocker.patch.object(bkClient, "sendXMLBookkeepingReport", return_value=S_OK())
        mock_bk_client.return_value = bkClient

        # BK registration failure
        mocker.patch(
            "DIRAC.Resources.Catalog.FileCatalog.FileCatalog.__getattr__",
            return_value=lambda x: S_ERROR("Error registering file"),
        )

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": "Tier1-Buffer",
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        # BK registration failure
        mocker.patch(
            "DIRAC.Resources.Catalog.FileCatalog.FileCatalog.__getattr__",
            return_value=lambda x: S_ERROR("Error registering file"),
        )

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute module
        with pytest.raises(WorkflowProcessingException, match="Could Not Perform BK Registration"):
            upload_output.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        assert fr.setFileStatus.call_count == 0
        assert bkClient.sendXMLBookkeepingReport.call_count == 1

        assert failover.transferAndRegisterFile.call_count == 1
        assert failover.transferAndRegisterFile.call_args[1]["fileName"] == sim_file

        assert failover.transferAndRegisterFileFailover.call_count == 0

        assert jr.setJobParameter.call_count == 1
        assert jr.setJobParameter.call_args[0][0] == "UploadedOutputData"
        assert jr.setJobParameter.call_args[0][1] == sim_file

        # Make sure the request is generated
        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 0

    def test_uploadOutputData_failUpload1(self, mocker, upload_output, wf_commons, sim_file, bk_file):
        """Test execution of UploadOutputData module when there is a 1st failure to upload outputs.

        * The output should be uploaded correctly with the second method.
        """
        mock_file_report = mocker.patch("dirac_cwl.commands.upload_output_data.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_output_data.JobReport")
        mock_request = mocker.patch("dirac_cwl.commands.upload_output_data.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_output_data.FailoverTransfer")
        mock_bk_client = mocker.patch("dirac_cwl.commands.upload_output_data.BookkeepingClient")

        fr = FileReport()
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setJobParameter")
        mock_job_report.return_value = jr

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(failover, "transferAndRegisterFile", return_value=S_ERROR("Error uploading file"))
        mocker.patch.object(failover, "transferAndRegisterFileFailover", return_value=S_OK())
        mock_failover.return_value = failover

        bkClient = BookkeepingClient()
        mocker.patch.object(bkClient, "sendXMLBookkeepingReport", return_value=S_OK())
        mock_bk_client.return_value = bkClient

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": "Tier1-Buffer",
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute module
        upload_output.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        assert fr.setFileStatus.call_count == 0
        assert bkClient.sendXMLBookkeepingReport.call_count == 1

        assert failover.transferAndRegisterFile.call_count == 1
        assert failover.transferAndRegisterFile.call_args[1]["fileName"] == sim_file

        assert failover.transferAndRegisterFileFailover.call_count == 1
        assert failover.transferAndRegisterFileFailover.call_args[1]["fileName"] == sim_file

        assert jr.setJobParameter.call_count == 1
        assert jr.setJobParameter.call_args[0][0] == "UploadedOutputData"
        assert jr.setJobParameter.call_args[0][1] == sim_file

        # Make sure the request is not generated
        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 0

    def test_uploadOutputData_failUpload2(self, mocker, upload_output, wf_commons, sim_file, bk_file):
        """Test execution of UploadOutputData module when there is a 2 failures to upload outputs.

        * A request should be generated to upload outputs later.
        """
        mock_file_report = mocker.patch("dirac_cwl.commands.upload_output_data.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_output_data.JobReport")
        mock_request = mocker.patch("dirac_cwl.commands.upload_output_data.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_output_data.FailoverTransfer")
        mock_bk_client = mocker.patch("dirac_cwl.commands.upload_output_data.BookkeepingClient")

        fr = FileReport()
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setJobParameter")
        mock_job_report.return_value = jr

        # Mock a previous failover request:
        # Add the end of the execution, o1 should be removed
        req = Request()

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

        req.addOperation(o1)
        req.addOperation(o2)

        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(failover, "transferAndRegisterFile", return_value=S_ERROR("Error uploading file"))
        mocker.patch.object(failover, "transferAndRegisterFileFailover", return_value=S_ERROR("Error uploading file"))
        mock_failover.return_value = failover

        bkClient = BookkeepingClient()
        mocker.patch.object(bkClient, "sendXMLBookkeepingReport", return_value=S_OK())
        mock_bk_client.return_value = bkClient

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": "Tier1-Buffer",
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute module
        with pytest.raises(WorkflowProcessingException, match="Failed to upload output data"):
            upload_output.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        assert fr.setFileStatus.call_count == 0
        assert bkClient.sendXMLBookkeepingReport.call_count == 1

        assert failover.transferAndRegisterFile.call_count == 1
        assert failover.transferAndRegisterFile.call_args[1]["fileName"] == sim_file

        assert failover.transferAndRegisterFileFailover.call_count == 1
        assert failover.transferAndRegisterFileFailover.call_args[1]["fileName"] == sim_file

        assert jr.setJobParameter.call_count == 0

        # Make sure the request is generated

        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 2

        assert operations[0]["Type"] == "RegisterFile"
        assert operations[0]["TargetSE"] is None
        assert operations[0]["SourceSE"] is None
        assert sim_file not in operations[0]["Files"][0]["LFN"]

        assert operations[1]["Type"] == "RemoveFile"
        assert operations[1]["TargetSE"] is None
        assert operations[1]["SourceSE"] is None
        assert sim_file in operations[1]["Files"][0]["LFN"]

    def test_uploadOutputData_BKReportError(self, mocker, upload_output, wf_commons, sim_file, bk_file):
        """Test execution of UploadOutputData module when the BK report cannot be sent.

        * The output should be uploaded and registered in the bookkeeping system.
        * The bookkeeping report should be added to a failover request.
        """
        mock_file_report = mocker.patch("dirac_cwl.commands.upload_output_data.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_output_data.JobReport")
        mock_request = mocker.patch("dirac_cwl.commands.upload_output_data.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_output_data.FailoverTransfer")
        mock_bk_client = mocker.patch("dirac_cwl.commands.upload_output_data.BookkeepingClient")

        fr = FileReport()
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setJobParameter")
        mock_job_report.return_value = jr

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(
            failover, "transferAndRegisterFile", return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file})
        )
        mocker.patch.object(failover, "transferAndRegisterFileFailover", return_value=S_ERROR("Error uploading file"))
        mock_failover.return_value = failover

        bkClient = BookkeepingClient()
        # Mock the sendXMLBookkeepingReport method
        mocker.patch.object(
            bkClient,
            "sendXMLBookkeepingReport",
            return_value={"OK": False, "rpcStub": "Error", "Message": "Error sending BK report"},
        )
        mock_bk_client.return_value = bkClient

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": "Tier1-Buffer",
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute module
        upload_output.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        assert fr.setFileStatus.call_count == 0
        assert bkClient.sendXMLBookkeepingReport.call_count == 1

        assert failover.transferAndRegisterFile.call_count == 1
        assert failover.transferAndRegisterFile.call_args[1]["fileName"] == sim_file

        assert failover.transferAndRegisterFileFailover.call_count == 0

        assert jr.setJobParameter.call_count == 1
        assert jr.setJobParameter.call_args[0][0] == "UploadedOutputData"
        assert jr.setJobParameter.call_args[0][1] == sim_file

        # Make sure the request is not generated
        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 1

        assert operations[0]["Type"] == "ForwardDISET"

    def test_uploadOutputData_withDescendents(self, mocker, upload_output, wf_commons, sim_file, bk_file):
        """Test execution of UploadOutputData module when there is already file descendants.

        It means that the input data has already been processed.
        * The output should not be uploaded and registered in the bookkeeping system.
        * The bookkeeping report should not be sent.
        """
        mock_file_report = mocker.patch("dirac_cwl.commands.upload_output_data.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_output_data.JobReport")
        mock_request = mocker.patch("dirac_cwl.commands.upload_output_data.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_output_data.FailoverTransfer")
        mock_bk_client = mocker.patch("dirac_cwl.commands.upload_output_data.BookkeepingClient")

        mocker.patch(
            "dirac_cwl.commands.upload_output_data.getFileDescendents", return_value=S_OK(["/path/to/other/file.txt"])
        )

        fr = FileReport()
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setJobParameter")
        mock_job_report.return_value = jr

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(
            failover, "transferAndRegisterFile", return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file})
        )
        mocker.patch.object(failover, "transferAndRegisterFileFailover")
        mock_failover.return_value = failover

        bkClient = BookkeepingClient()
        mocker.patch.object(bkClient, "sendXMLBookkeepingReport")
        mock_bk_client.return_value = bkClient

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": "Tier1-Buffer",
        }
        wf_commons["inputs"] = ["AnyInputFile1"]
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute module
        with pytest.raises(WorkflowProcessingException):
            upload_output.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        assert fr.setFileStatus.call_count == 1
        assert fr.setFileStatus.call_args[0][0] == int(wf_commons["production_id"])
        assert bkClient.sendXMLBookkeepingReport.call_count == 0

        assert failover.transferAndRegisterFile.call_count == 0
        assert failover.transferAndRegisterFileFailover.call_count == 0

        assert jr.setJobParameter.call_count == 0

        # Make sure the request is not generated
        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 0

    def test_uploadOutputData_noOutput(self, mocker, upload_output, wf_commons, sim_file):
        """Test UploadOutputData with no output data."""
        mock_file_report = mocker.patch("dirac_cwl.commands.upload_output_data.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_output_data.JobReport")
        mock_request = mocker.patch("dirac_cwl.commands.upload_output_data.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_output_data.FailoverTransfer")
        mock_bk_client = mocker.patch("dirac_cwl.commands.upload_output_data.BookkeepingClient")

        fr = FileReport()
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setJobParameter")
        mock_job_report.return_value = jr

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(
            failover, "transferAndRegisterFile", return_value=S_OK({"uploadedSE": "CERN", "lfn": sim_file})
        )
        mocker.patch.object(failover, "transferAndRegisterFileFailover")
        mock_failover.return_value = failover

        bkClient = BookkeepingClient()
        mocker.patch.object(bkClient, "sendXMLBookkeepingReport")
        mock_bk_client.return_value = bkClient

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": "Tier1-Buffer",
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        # Remove the output
        Path(sim_file).unlink(missing_ok=True)

        wf_commons_path = create_workflow_commons(wf_commons)

        # Execute module
        with pytest.raises(WorkflowProcessingException, match="Output data not found"):
            upload_output.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        assert fr.setFileStatus.call_count == 0
        assert bkClient.sendXMLBookkeepingReport.call_count == 0

        assert failover.transferAndRegisterFile.call_count == 0
        assert failover.transferAndRegisterFileFailover.call_count == 0

        assert jr.setJobParameter.call_count == 0

        # Make sure the request is not generated
        print(updated_wf_commons)
        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 0

    def test_uploadOutputData_previousError_fail(self, mocker, upload_output, wf_commons, sim_file):
        """Test UploadOutputData with an intentional failure."""
        mock_file_report = mocker.patch("dirac_cwl.commands.upload_output_data.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_output_data.JobReport")
        mock_request = mocker.patch("dirac_cwl.commands.upload_output_data.Request")
        mock_failover = mocker.patch("dirac_cwl.commands.upload_output_data.FailoverTransfer")
        mock_bk_client = mocker.patch("dirac_cwl.commands.upload_output_data.BookkeepingClient")

        fr = FileReport()
        mocker.patch.object(fr, "setFileStatus")
        mock_file_report.return_value = fr

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setJobParameter")
        mock_job_report.return_value = jr

        req = Request()
        mock_request.return_value = req

        failover = FailoverTransfer(req)
        mocker.patch.object(failover, "transferAndRegisterFile")
        mocker.patch.object(failover, "transferAndRegisterFileFailover")
        mock_failover.return_value = failover

        bkClient = BookkeepingClient()
        mocker.patch.object(bkClient, "sendXMLBookkeepingReport")
        mock_bk_client.return_value = bkClient

        wf_commons["outputs"] = [
            {"outputDataName": sim_file, "outputDataType": "sim", "outputBKType": "SIM", "stepName": "Gauss_1"}
        ]
        wf_commons["output_SEs"] = {
            "SIM": "Tier1-Buffer",
        }
        wf_commons["output_data_step"] = self.OUTPUT_DATA_STEP

        wf_commons["step_status"] = S_ERROR()

        Path(sim_file).unlink(missing_ok=True)

        wf_commons_path = create_workflow_commons(wf_commons)

        upload_output.execute(job_path)

        with open(wf_commons_path, "r", encoding="utf-8") as f:
            updated_wf_commons = json.load(f)

        assert fr.setFileStatus.call_count == 0
        assert bkClient.sendXMLBookkeepingReport.call_count == 0

        assert failover.transferAndRegisterFile.call_count == 0
        assert failover.transferAndRegisterFileFailover.call_count == 0

        assert jr.setJobParameter.call_count == 0

        # Make sure the request is not generated
        operations = updated_wf_commons["request_dict"]["Operations"]
        assert len(operations) == 0


class TestAnalyseXmlSummary:
    """Collection of tests for the AnalyseXmlSummary command."""

    @pytest.fixture
    def axlf(self, mocker):
        """Fixture for AnalyseXmlSummary module."""
        yield AnalyseXmlSummary()

    # Test scenarios
    def test_analyseXMLSummary_basic_success(self, mocker, axlf, wf_commons, xml_summary_file):
        """Test basic success scenario."""
        mock_file_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.JobReport")

        fr = FileReport()

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        jr.setApplicationStatus.return_value = S_OK()

        mock_file_report.return_value = fr
        mock_job_report.return_value = jr

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="full">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)
        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        create_workflow_commons(wf_commons)
        axlf.execute(job_path)

        jr.setApplicationStatus.assert_called_once()
        assert fr.statusDict == {}

    def test_analyseXMLSummary_previousError_success(self, mocker, axlf, wf_commons, xml_summary_file):
        """Test success scenario with previous error: stepStatus = S_ERROR()."""
        mock_file_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.JobReport")

        fr = FileReport()

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        jr.setApplicationStatus.return_value = S_OK()

        mock_file_report.return_value = fr
        mock_job_report.return_value = jr

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="full">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)
        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["step_status"] = S_ERROR()
        wf_commons["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        create_workflow_commons(wf_commons)
        axlf.execute(job_path)

        jr.setApplicationStatus.assert_not_called()
        assert fr.statusDict == {}

    def test_analyseXMLSummary_badInput_success(self, mocker, axlf, wf_commons, xml_summary_file):
        """Test success scenario with part and fail input not part of the input data list."""
        mock_file_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.JobReport")

        fr = FileReport()

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        jr.setApplicationStatus.return_value = S_OK()

        mock_file_report.return_value = fr
        mock_job_report.return_value = jr

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="part">200</file>
                            <file GUID="CCE96809-4FC6-F623-61F5-003048F35253" name="LFN:00012478_00000533_1.sim"
                            status="fail">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)
        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        create_workflow_commons(wf_commons)
        axlf.execute(job_path)

        jr.setApplicationStatus.assert_called_once()
        assert fr.statusDict == {}

    def test_analyseXMLSummary_partInput_success(self, mocker, axlf, wf_commons, xml_summary_file):
        """Test success scenario with part input part of the input data list."""
        # Input is 'part' and is part of the input data list but the number of events is not -1
        mock_file_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.JobReport")

        fr = FileReport()

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        jr.setApplicationStatus.return_value = S_OK()

        mock_file_report.return_value = fr
        mock_job_report.return_value = jr

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="part">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)
        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)
        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["inputs"] = ["00012478_00000532_1.sim"]
        wf_commons["number_of_events"] = 1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        create_workflow_commons(wf_commons)
        axlf.execute(job_path)

        jr.setApplicationStatus.assert_called_once()
        assert fr.statusDict == {}

    def test_analyseXMLSummary_notSuccess_fail(self, mocker, axlf, wf_commons, xml_summary_file):
        """Test failure scenario with success=False."""
        mock_file_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.JobReport")

        fr = FileReport()

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        jr.setApplicationStatus.return_value = S_OK()

        mock_file_report.return_value = fr
        mock_job_report.return_value = jr

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>False</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="part">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)
        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["number_of_events"] = -1

        assert xf_o.success == "False"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        create_workflow_commons(wf_commons)
        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        jr.setApplicationStatus.assert_called_once()
        assert fr.statusDict == {}

    def test_analyseXMLSummary_badStep_fail(self, mocker, axlf, wf_commons, xml_summary_file):
        """Test failure scenario with step != finalize."""
        mock_file_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.JobReport")

        fr = FileReport()

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        jr.setApplicationStatus.return_value = S_OK()

        mock_file_report.return_value = fr
        mock_job_report.return_value = jr

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>execute</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="part">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)
        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "execute"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        create_workflow_commons(wf_commons)
        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        jr.setApplicationStatus.assert_called_once()
        assert fr.statusDict == {}

    def test_analyseXMLSummary_badOutput_fail(self, mocker, axlf, wf_commons, xml_summary_file):
        """Test failure scenario with output status != full."""
        mock_file_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.JobReport")

        fr = FileReport()

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        jr.setApplicationStatus.return_value = S_OK()

        mock_file_report.return_value = fr
        mock_job_report.return_value = jr

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="part">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="fail">200</file>
                    </output>
            </summary>
            """)

        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)
        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert not xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        create_workflow_commons(wf_commons)
        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        jr.setApplicationStatus.assert_called_once()
        assert fr.statusDict == {}

    def test_analyseXMLSummary_badInput_fail(self, mocker, axlf, wf_commons, xml_summary_file):
        """Test failure scenario with input status = mult."""
        mock_file_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.JobReport")

        fr = FileReport()

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        jr.setApplicationStatus.return_value = S_OK()

        mock_file_report.return_value = fr
        mock_job_report.return_value = jr

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="mult">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)
        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        create_workflow_commons(wf_commons)
        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        jr.setApplicationStatus.assert_called_once()
        assert fr.statusDict == {}

    def test_analyseXMLSummary_badInput2_fail(self, mocker, axlf, wf_commons, xml_summary_file):
        """Test failure scenario with an unknown input status (weoweo)."""
        mock_file_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.JobReport")

        fr = FileReport()

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        jr.setApplicationStatus.return_value = S_OK()

        mock_file_report.return_value = fr
        mock_job_report.return_value = jr

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="weoweo">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)
        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert xf_o.inputFileStats["other"]

        create_workflow_commons(wf_commons)
        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        jr.setApplicationStatus.assert_called_once()
        assert fr.statusDict == {}

    def test_analyseXMLSummary_badInput3_fail(self, mocker, axlf, wf_commons, xml_summary_file):
        """Test failure scenario with input status = fail."""
        mock_file_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.JobReport")

        fr = FileReport()

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        jr.setApplicationStatus.return_value = S_OK()

        mock_file_report.return_value = fr
        mock_job_report.return_value = jr

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="fail">200</file>
                            <file GUID="CCE96709-5BE9-E012-41BD-004048E36253" name="LFN:00012478_00000533_1.sim"
                            status="fail">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)
        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["inputs"] = ["00012478_00000532_1.sim"]
        wf_commons["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        create_workflow_commons(wf_commons)
        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        jr.setApplicationStatus.assert_called_once()
        assert fr.statusDict == {"00012478_00000532_1.sim": "Problematic"}

    def test_analyseXMLSummary_badInput4_fail(self, mocker, axlf, wf_commons, xml_summary_file):
        """Test failure scenario with input status = part."""
        # Input is 'part' and is part of the input data list but the number of events is -1 (by default)
        mock_file_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.FileReport")
        mock_job_report = mocker.patch("dirac_cwl.commands.analyze_xml_summary.JobReport")

        fr = FileReport()

        jr = JobReport(wf_commons["job_id"])
        mocker.patch.object(jr, "setApplicationStatus")
        jr.setApplicationStatus.return_value = S_OK()

        mock_file_report.return_value = fr
        mock_job_report.return_value = jr

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="part">200</file>
                            <file GUID="CCE96709-5BE9-E012-41BD-004048E36253" name="LFN:00012478_00000533_1.sim"
                            status="part">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        xf_o = prepare_XMLSummary_file(xml_summary_file, xml_content)
        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["inputs"] = ["00012478_00000532_1.sim"]
        wf_commons["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        create_workflow_commons(wf_commons)
        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        jr.setApplicationStatus.assert_called_once()
        assert fr.statusDict == {"00012478_00000532_1.sim": "Problematic"}


class TestWorkflowAccounting:
    """Collection of tests for the WorkflowAccounting command."""

    @pytest.fixture
    def accounting(self, mocker):
        """Fixture for WorkflowAccounting module."""
        yield WorkflowAccounting()

    # Test Scenarios
    def test_accounting_success(self, mocker, accounting, wf_commons, xml_summary_file):
        """Test successful execution of WorkflowAccounting module."""
        mock_data_store = mocker.patch("dirac_cwl.commands.workflow_accounting.DataStoreClient")
        dsc = DataStoreClient()
        mocker.patch.object(dsc, "addRegister")
        mock_data_store.return_value = dsc

        wf_commons["application_name"] = "Gauss"
        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="full">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        prepare_XMLSummary_file(xml_summary_file, xml_content)

        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["bk_step_id"] = "12345"
        wf_commons["step_proc_pass"] = "Sim09m"
        wf_commons["event_type"] = "23103003"

        create_workflow_commons(wf_commons)

        accounting.execute(job_path)

        # Make sure the dsc was called
        dsc.addRegister.assert_called_once()

    def test_accounting_noApplicationName_fail(self, mocker, accounting, wf_commons, xml_summary_file):
        """Test WorkflowAccounting when there is no application name in step commons."""
        mock_data_store = mocker.patch("dirac_cwl.commands.workflow_accounting.DataStoreClient")
        dsc = DataStoreClient()
        mocker.patch.object(dsc, "addRegister")
        mock_data_store.return_value = dsc

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="full">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        prepare_XMLSummary_file(xml_summary_file, xml_content)

        wf_commons.pop("application_name")
        wf_commons["xml_summary_path"] = xml_summary_file

        create_workflow_commons(wf_commons)

        with pytest.raises(WorkflowProcessingException):
            accounting.execute(job_path)

        assert not dsc.addRegister.called, "No accounting data should be added."

    def test_accounting_incompleteData(self, mocker, accounting, wf_commons, xml_summary_file):
        """Test successful execution of WorkflowAccounting module."""
        mock_data_store = mocker.patch("dirac_cwl.commands.workflow_accounting.DataStoreClient")
        dsc = DataStoreClient()
        mocker.patch.object(dsc, "addRegister")
        mock_data_store.return_value = dsc

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="full">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        prepare_XMLSummary_file(xml_summary_file, xml_content)

        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["application_name"] = "Gauss"

        create_workflow_commons(wf_commons)

        with pytest.raises(WorkflowProcessingException):
            accounting.execute(job_path)

        assert not dsc.addRegister.called, "No accounting data should be added."

    def test_accounting_previousError_fail(self, mocker, accounting, wf_commons, xml_summary_file):
        """Test WorkflowAccounting with an intentional failure."""
        mock_data_store = mocker.patch("dirac_cwl.commands.workflow_accounting.DataStoreClient")
        dsc = DataStoreClient()
        mocker.patch.object(dsc, "addRegister")
        mock_data_store.return_value = dsc

        xml_content = dedent("""<?xml version="1.0" encoding="UTF-8"?>
            <summary version="1.0" xsi:noNamespaceSchemaLocation="$XMLSUMMARYBASEROOT/xml/XMLSummary.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    <success>True</success>
                    <step>finalize</step>
                    <usage>
                            <stat unit="KB" useOf="MemoryMaximum">866104.0</stat>
                    </usage>
                    <input>
                            <file GUID="CCE96707-4BE9-E011-81CD-003048F35252" name="LFN:00012478_00000532_1.sim"
                            status="full">200</file>
                    </input>
                    <output>
                            <file GUID="229BBEF1-66E9-E011-BBD0-003048F35252" name="PFN:00012478_00000532_2.xdigi"
                            status="full">200</file>
                    </output>
            </summary>
            """)

        prepare_XMLSummary_file(xml_summary_file, xml_content)

        wf_commons["xml_summary_path"] = xml_summary_file
        wf_commons["application_name"] = "Gauss"
        wf_commons["bk_step_id"] = "12345"
        wf_commons["step_proc_pass"] = "Sim09m"
        wf_commons["event_type"] = "23103003"
        wf_commons["step_status"] = S_ERROR()

        create_workflow_commons(wf_commons)

        accounting.execute(job_path)

        assert dsc.addRegister.called, "Accounting data should be added."
