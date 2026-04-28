"""."""

import json
import os
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from textwrap import dedent
from urllib.parse import urljoin

import LHCbDIRAC
import pytest
from DIRAC import siteName
from DIRACCommon.Core.Utilities.ReturnValues import S_ERROR, S_OK
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary
from pytest_mock import MockerFixture

from dirac_cwl.commands import BookeepingReport, UploadLogFile


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
        mock_ops = mocker.patch("dirac_cwl.commands.upload_log_file.Operations")
        mock_ops.return_value.getValue = lambda value, default=None: default

        # Mock JobReport
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_log_file.JobReport")
        mock_set_app_status = mocker.MagicMock()
        mock_set_job_parameter = mocker.MagicMock()
        mock_job_report.return_value.setApplicationStatus = mock_set_app_status
        mock_job_report.return_value.setJobParameter = mock_set_job_parameter

        # Mock StorageElement
        mock_se = mocker.patch("dirac_cwl.commands.upload_log_file.StorageElement")
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
        mock_ops = mocker.patch("dirac_cwl.commands.upload_log_file.Operations")
        mock_ops.return_value.getValue = lambda value, default=None: default

        # Mock JobReport
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_log_file.JobReport")
        mock_set_app_status = mocker.MagicMock()
        mock_set_job_parameter = mocker.MagicMock()
        mock_job_report.return_value.setApplicationStatus = mock_set_app_status
        mock_job_report.return_value.setJobParameter = mock_set_job_parameter

        # Mock StorageElement
        mock_se = mocker.patch("dirac_cwl.commands.upload_log_file.StorageElement")
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
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_log_file.JobReport")
        mock_set_app_status = mocker.MagicMock()
        mock_set_job_parameter = mocker.MagicMock()
        mock_job_report.return_value.setApplicationStatus = mock_set_app_status
        mock_job_report.return_value.setJobParameter = mock_set_job_parameter

        # Mock StorageElement
        mock_se = mocker.patch("dirac_cwl.commands.upload_log_file.StorageElement")
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
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_log_file.JobReport")
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
        mock_job_report = mocker.patch("dirac_cwl.commands.upload_log_file.JobReport")
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


class TestBookkeepingReport:
    """Collection of tests for the TestBookkeepingReport command."""

    wms_job_id = 0
    job_type = "merge"
    production_id = "123"
    prod_job_id = "00000456"
    event_type = "123456789"
    number_of_events = "100"
    config_name = "aConfigName"
    config_version = "aConfigVersion"
    application_name = "someApp"
    application_version = "v1r0"
    bk_step_id = "123"
    command_id = "1"
    number_of_processors = 1
    job_path = "."

    xml_summary_file = os.path.join(
        job_path, f"summary{application_name}_{production_id}_{prod_job_id}_{command_id}.xml"
    )
    wf_commons_file = os.path.join(job_path, "workflow_commons.json")
    bookkeeping_file = os.path.join(job_path, f"bookkeeping_{command_id}.xml")

    @pytest.fixture
    def wf_commons(self):
        """Workflow Commons dictionary fixture."""
        content = {
            "job_id": self.wms_job_id,
            "job_type": self.job_type,
            "production_id": self.production_id,
            "prod_job_id": self.prod_job_id,
            "event_type": self.event_type,
            "number_of_events": self.number_of_events,
            "config_name": self.config_name,
            "config_version": self.config_version,
            "application_name": self.application_name,
            "application_version": self.application_version,
            "bk_step_id": self.bk_step_id,
            "inputs": [],
            "outputs": [],
            "executable": "",
            "command_id": self.command_id,
            "command_number": 1,
        }

        yield content

    @pytest.fixture
    def bk_report(self, mocker):
        """BookkeepingReport mocked command.

        Cleans created files after execution.
        """
        mock_get_n_procs = mocker.patch("dirac_cwl.commands.bookkeeping_report.getNumberOfProcessorsToUse")

        mock_get_n_procs.return_value = self.number_of_processors

        yield BookeepingReport()

        Path(self.wf_commons_file).unlink(missing_ok=True)
        Path(self.bookkeeping_file).unlink(missing_ok=True)
        Path(self.xml_summary_file).unlink(missing_ok=True)

        Path("00209455_00001537_1").unlink(missing_ok=True)
        Path("00209455_00001537_1.sim").unlink(missing_ok=True)

    def test_bkreport_prod_mcsimulation_success(self, bk_report, wf_commons):
        """Test successful execution of BookkeepingReport module."""
        wf_commons["application_name"] = "Gauss"
        wf_commons["application_version"] = self.application_version
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

        wf_commons["xml_summary_path"] = self.xml_summary_file
        xf_o = prepare_XMLSummary_file(self.xml_summary_file, xml_content)

        with open(self.wf_commons_file, "w", encoding="utf-8") as f:
            json.dump(wf_commons, f)

        # Execute the module
        bk_report.execute(self.job_path)

        xml_path = self.bookkeeping_file
        assert Path(xml_path).exists(), "XML report file not created."

        # Validate the XML file
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Extract fields from the XML and perform further operations
        assert root.tag == "Job", "Root tag should be Job."
        assert root.attrib["ConfigName"] == self.config_name
        assert root.attrib["ConfigVersion"] == self.config_version
        assert root.attrib["Date"]
        assert root.attrib["Time"]

        assert get_typed_parameter_value("ProgramName", root) == wf_commons["application_name"]
        assert get_typed_parameter_value("ProgramVersion", root) == wf_commons["application_version"]
        assert get_typed_parameter_value("DiracVersion", root) == LHCbDIRAC.__version__
        assert get_typed_parameter_value("Name", root) == self.command_id
        assert float(get_typed_parameter_value("ExecTime", root)) > 1000
        assert get_typed_parameter_value("CPUTIME", root) == "0"

        assert get_typed_parameter_value("FirstEventNumber", root) == "1"
        assert get_typed_parameter_value("StatisticsRequested", root) == str(wf_commons["number_of_events"])
        assert get_typed_parameter_value("NumberOfEvents", root) == str(xf_o.outputEventsTotal)

        assert get_typed_parameter_value("Production", root) == self.production_id
        assert get_typed_parameter_value("DiracJobId", root) == str(self.wms_job_id)
        assert get_typed_parameter_value("Location", root) == siteName()
        assert get_typed_parameter_value("JobStart", root)
        assert get_typed_parameter_value("JobEnd", root)
        assert get_typed_parameter_value("JobType", root) == wf_commons["job_type"]

        assert get_typed_parameter_value("WorkerNode", root)
        assert get_typed_parameter_value("WNMEMORY", root)
        assert get_typed_parameter_value("WNCPUPOWER", root)
        assert get_typed_parameter_value("WNMODEL", root)
        assert get_typed_parameter_value("WNCACHE", root)
        assert get_typed_parameter_value("WNCPUHS06", root)
        assert get_typed_parameter_value("NumberOfProcessors", root) == str(self.number_of_processors)

        # Input should be empty
        input_file = root.find("InputFile")
        assert input_file is None, "InputFile element should not be present."

        # Output should not be empty
        output_files = root.findall("OutputFile")
        assert output_files, "No OutputFile elements found."

        first_output_details = get_output_file_details(output_files[0])
        assert first_output_details["Name"] == wf_commons["production_output_data"][0]
        assert first_output_details["TypeName"] == "SIM"
        assert first_output_details["Parameters"]["FileSize"] == "0"
        assert "CreationDate" in first_output_details["Parameters"]
        assert "MD5Sum" in first_output_details["Parameters"]
        assert "Guid" in first_output_details["Parameters"]

        assert len(output_files) == 1

    def test_bkreport_prod_mcsimulation_noinputoutput_success(self, bk_report, wf_commons):
        """Test successful execution of BookkeepingReport module.

        * No input files because wf_commons["stepInputData is empty
        * No output files because wf_commons["stepOutputData is empty
        * No pool xml catalog
        * Simulation conditions because the application used is Gauss
        """
        # Mock the BookkeepingReport module
        wf_commons["application_name"] = "Gauss"
        wf_commons["application_version"] = self.application_version
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

        wf_commons["xml_summary_path"] = self.xml_summary_file
        xf_o = prepare_XMLSummary_file(self.xml_summary_file, xml_content)

        with open(self.wf_commons_file, "w", encoding="utf-8") as f:
            json.dump(wf_commons, f)

        # Execute the module
        bk_report.execute(self.job_path)

        # Check if the XML report file is created
        xml_path = self.bookkeeping_file
        assert Path(xml_path).exists(), "XML report file not created."

        # Validate the XML file
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Extract fields from the XML and perform further operations
        assert root.tag == "Job", "Root tag should be Job."
        assert root.attrib["ConfigName"] == self.config_name
        assert root.attrib["ConfigVersion"] == self.config_version
        assert root.attrib["Date"]
        assert root.attrib["Time"]

        assert get_typed_parameter_value("ProgramName", root) == wf_commons["application_name"]
        assert get_typed_parameter_value("ProgramVersion", root) == wf_commons["application_version"]
        assert get_typed_parameter_value("DiracVersion", root) == LHCbDIRAC.__version__
        assert get_typed_parameter_value("Name", root) == self.command_id
        assert float(get_typed_parameter_value("ExecTime", root)) > 1000
        assert get_typed_parameter_value("CPUTIME", root) == "0"

        assert get_typed_parameter_value("FirstEventNumber", root) == "1"
        assert get_typed_parameter_value("StatisticsRequested", root) == str(wf_commons["number_of_events"])
        assert get_typed_parameter_value("NumberOfEvents", root) == str(xf_o.outputEventsTotal)

        assert get_typed_parameter_value("Production", root) == self.production_id
        assert get_typed_parameter_value("DiracJobId", root) == str(self.wms_job_id)
        assert get_typed_parameter_value("Location", root) == siteName()
        assert get_typed_parameter_value("JobStart", root)
        assert get_typed_parameter_value("JobEnd", root)
        assert get_typed_parameter_value("JobType", root) == wf_commons["job_type"]

        assert get_typed_parameter_value("WorkerNode", root)
        assert get_typed_parameter_value("WNMEMORY", root)
        assert get_typed_parameter_value("WNCPUPOWER", root)
        assert get_typed_parameter_value("WNMODEL", root)
        assert get_typed_parameter_value("WNCACHE", root)
        assert get_typed_parameter_value("WNCPUHS06", root)
        assert get_typed_parameter_value("NumberOfProcessors", root) == str(self.number_of_processors)

        # Input should be empty
        input_file = root.find("InputFile")
        assert input_file is None, "InputFile element should not be present."

        # Output should be empty
        output_file = root.find("OutputFile")
        assert output_file is None, "OutputFile element should not be present."

    def test_bk_report_prod_mcreconstruction_success(self, bk_report, wf_commons):
        """Test successful execution of BookkeepingReport module."""
        wf_commons["application_name"] = "Boole"
        wf_commons["application_version"] = self.application_version
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

        wf_commons["xml_summary_path"] = self.xml_summary_file

        xf_o = prepare_XMLSummary_file(self.xml_summary_file, xml_content)

        with open(self.wf_commons_file, "w", encoding="utf-8") as f:
            json.dump(wf_commons, f)

        # Execute the module
        bk_report.execute(self.job_path)

        # Check if the XML report file is created
        xml_path = self.bookkeeping_file
        assert Path(xml_path).exists(), "XML report file not created."

        # Validate the XML file
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Extract fields from the XML and perform further operations
        assert root.tag == "Job", "Root tag should be Job."
        assert root.attrib["ConfigName"] == self.config_name
        assert root.attrib["ConfigVersion"] == self.config_version
        assert root.attrib["Date"]
        assert root.attrib["Time"]

        assert get_typed_parameter_value("ProgramName", root) == wf_commons["application_name"]
        assert get_typed_parameter_value("ProgramVersion", root) == wf_commons["application_version"]
        assert get_typed_parameter_value("DiracVersion", root) == LHCbDIRAC.__version__
        assert get_typed_parameter_value("Name", root) == self.command_id
        assert float(get_typed_parameter_value("ExecTime", root)) > 1000
        assert get_typed_parameter_value("CPUTIME", root) == "0"

        assert get_typed_parameter_value("FirstEventNumber", root) == "1"
        assert get_typed_parameter_value("StatisticsRequested", root) == str(wf_commons["number_of_events"])
        assert get_typed_parameter_value("NumberOfEvents", root) == str(xf_o.inputEventsTotal)

        assert get_typed_parameter_value("Production", root) == self.production_id
        assert get_typed_parameter_value("DiracJobId", root) == str(self.wms_job_id)
        assert get_typed_parameter_value("Location", root) == siteName()
        assert get_typed_parameter_value("JobStart", root)
        assert get_typed_parameter_value("JobEnd", root)
        assert get_typed_parameter_value("JobType", root) == wf_commons["job_type"]

        assert get_typed_parameter_value("WorkerNode", root)
        assert get_typed_parameter_value("WNMEMORY", root)
        assert get_typed_parameter_value("WNCPUPOWER", root)
        assert get_typed_parameter_value("WNMODEL", root)
        assert get_typed_parameter_value("WNCACHE", root)
        assert get_typed_parameter_value("WNCPUHS06", root)
        assert get_typed_parameter_value("NumberOfProcessors", root) == str(self.number_of_processors)

        # Input should not be empty
        input_file = root.find("InputFile")
        assert input_file is not None, "InputFile element should be present."

        # Output should not be empty
        output_files = root.findall("OutputFile")
        assert output_files, "No OutputFile elements found."

        first_output_details = get_output_file_details(output_files[0])
        assert first_output_details["Name"] == wf_commons["production_output_data"][0]
        assert first_output_details["TypeName"] == "DIGI"
        assert first_output_details["Parameters"]["FileSize"] == "0"
        assert "CreationDate" in first_output_details["Parameters"]
        assert "MD5Sum" in first_output_details["Parameters"]
        assert "Guid" in first_output_details["Parameters"]

        assert len(output_files) == 1

        # Because we are using Gauss, sim conditions should be present too
        simulation_condition = root.find("SimulationCondition")
        assert simulation_condition is None, "SimulationCondition element should not be present."

    def test_bkreport_previousError_success(self, mocker, bk_report, wf_commons):
        """."""
        wf_commons["application_name"] = "Gauss"
        wf_commons["application_version"] = self.config_version
        wf_commons["job_type"] = "MCSimulation"
        wf_commons["step_status"] = S_ERROR()

        with open(self.wf_commons_file, "w", encoding="utf-8") as f:
            json.dump(wf_commons, f)

        bk_report.execute(self.job_path)

        assert not os.path.exists(self.bookkeeping_file)
