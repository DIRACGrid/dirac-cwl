"""Test for the ReportBookkeeping command class."""

import time
import xml.etree.ElementTree as ET
from pathlib import Path
from textwrap import dedent

import LHCbDIRAC
import pytest
from DIRAC import siteName
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary
from pytest_mock import MockerFixture

from dirac_cwl.commands import ReportBookkeeping
from dirac_cwl.commands.workflow_commons import StepStatus, WorkflowCommons


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


class TestReportBookkeeping:
    """Collection of tests for the ReportBookkeeping command."""

    number_of_processors = 1

    @pytest.fixture
    def report_bk(self, mocker: MockerFixture, job_path):
        """ReportBookkeeping mocked command.

        Cleans created files after execution.
        """
        mock_get_n_procs = mocker.patch("dirac_cwl.commands.report_bookkeeping.getNumberOfProcessorsToUse")
        mock_get_n_procs.return_value = self.number_of_processors

        yield ReportBookkeeping()

        Path(job_path).joinpath("00209455_00001537_1").unlink(missing_ok=True)
        Path(job_path).joinpath("00209455_00001537_1.sim").unlink(missing_ok=True)
        Path(job_path).joinpath("application.log").unlink(missing_ok=True)
        Path(job_path).joinpath("workflow_commons.json").unlink(missing_ok=True)

    def test_report_bk_prod_mcsimulation_success(
        self, report_bk, job_path, wf_commons, bookkeeping_file, xml_summary_file
    ):
        """Test successful execution of ReportBookkeeping module."""
        wf_commons["steps"][0]["application_name"] = "Gauss"
        wf_commons["job_type"] = "MCSimulation"

        wf_commons["bookkeeping_lfns"] = [
            "/lhcb/LHCb/Collision16/SIM/00209455/0000/00209455_00001537_1.sim",
        ]
        wf_commons["production_output_data"] = [
            "/lhcb/LHCb/Collision16/SIM/00209455/0000/00209455_00001537_1.sim",
        ]

        wf_commons["steps"][0]["start_time"] = time.time() - 1000

        # Input data should be None as we use Gauss (MCSimulation)
        wf_commons["steps"][0]["outputs"] = [
            {"outputDataName": "00209455_00001537_1.sim", "outputDataType": "sim"},
        ]
        Path(wf_commons["steps"][0]["outputs"][0]["outputDataName"]).touch()

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file

        WorkflowCommons(**wf_commons).save(job_path)

        report_bk.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        assert Path(bookkeeping_file).exists(), "XML report file not created."

        # Validate the XML file
        tree = ET.parse(bookkeeping_file)
        root = tree.getroot()

        # Extract fields from the XML and perform further operations
        assert root.tag == "Job", "Root tag should be Job."
        assert root.attrib["ConfigName"] == updated_wf_commons.config_name
        assert root.attrib["ConfigVersion"] == updated_wf_commons.config_version
        assert root.attrib["Date"]
        assert root.attrib["Time"]

        assert get_typed_parameter_value("ProgramName", root) == updated_wf_commons.steps[0].application_name
        assert get_typed_parameter_value("ProgramVersion", root) == updated_wf_commons.steps[0].application_version
        assert get_typed_parameter_value("DiracVersion", root) == LHCbDIRAC.__version__
        assert get_typed_parameter_value("Name", root) == updated_wf_commons.steps[0].id
        assert float(get_typed_parameter_value("ExecTime", root)) > 1000
        assert get_typed_parameter_value("CPUTIME", root) == "0"

        assert get_typed_parameter_value("FirstEventNumber", root) == "1"
        assert get_typed_parameter_value("StatisticsRequested", root) == str(
            updated_wf_commons.steps[0].number_of_events
        )
        assert get_typed_parameter_value("NumberOfEvents", root) == str(xf_o.outputEventsTotal)

        assert get_typed_parameter_value("Production", root) == updated_wf_commons.production_id
        assert get_typed_parameter_value("DiracJobId", root) == str(updated_wf_commons.job_id)
        assert get_typed_parameter_value("Location", root) == siteName()
        assert get_typed_parameter_value("JobStart", root)
        assert get_typed_parameter_value("JobEnd", root)
        assert get_typed_parameter_value("JobType", root) == updated_wf_commons.job_type

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
        assert first_output_details["Name"] == updated_wf_commons.production_output_data[0]
        assert first_output_details["TypeName"] == "SIM"
        assert first_output_details["Parameters"]["FileSize"] == "0"
        assert "CreationDate" in first_output_details["Parameters"]
        assert "MD5Sum" in first_output_details["Parameters"]
        assert "Guid" in first_output_details["Parameters"]

        assert len(output_files) == 1

    def test_report_bk_prod_mcsimulation_noinputoutput_success(
        self, report_bk, job_path, wf_commons, bookkeeping_file, xml_summary_file
    ):
        """Test successful execution of ReportBookkeeping module.

        * No input files because wf_commons["stepInputData is empty
        * No output files because wf_commons["stepOutputData is empty
        * No pool xml catalog
        * Simulation conditions because the application used is Gauss
        """
        # Mock the ReportBookkeeping module
        wf_commons["steps"][0]["application_name"] = "Gauss"
        wf_commons["job_type"] = "MCSimulation"

        # This was obtained from a previous module (likely GaudiApplication)
        wf_commons["bookkeeping_lfns"] = [
            "/lhcb/LHCb/Collision16/SIM/00209455/0000/00209455_00001537_1",
        ]
        wf_commons["production_output_data"] = [
            "/lhcb/LHCb/Collision16/SIM/00209455/0000/00209455_00001537_1",
        ]

        wf_commons["steps"][0]["start_time"] = time.time() - 1000

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file

        WorkflowCommons(**wf_commons).save(job_path)

        report_bk.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        # Check if the XML report file is created
        assert Path(bookkeeping_file).exists(), "XML report file not created."

        # Validate the XML file
        tree = ET.parse(bookkeeping_file)
        root = tree.getroot()

        # Extract fields from the XML and perform further operations
        assert root.tag == "Job", "Root tag should be Job."
        assert root.attrib["ConfigName"] == updated_wf_commons.config_name
        assert root.attrib["ConfigVersion"] == updated_wf_commons.config_version
        assert root.attrib["Date"]
        assert root.attrib["Time"]

        assert get_typed_parameter_value("ProgramName", root) == updated_wf_commons.steps[0].application_name
        assert get_typed_parameter_value("ProgramVersion", root) == updated_wf_commons.steps[0].application_version
        assert get_typed_parameter_value("DiracVersion", root) == LHCbDIRAC.__version__
        assert get_typed_parameter_value("Name", root) == updated_wf_commons.steps[0].id
        assert float(get_typed_parameter_value("ExecTime", root)) > 1000
        assert get_typed_parameter_value("CPUTIME", root) == "0"

        assert get_typed_parameter_value("FirstEventNumber", root) == "1"
        assert get_typed_parameter_value("StatisticsRequested", root) == str(
            updated_wf_commons.steps[0].number_of_events
        )
        assert get_typed_parameter_value("NumberOfEvents", root) == str(xf_o.outputEventsTotal)

        assert get_typed_parameter_value("Production", root) == updated_wf_commons.production_id
        assert get_typed_parameter_value("DiracJobId", root) == str(updated_wf_commons.job_id)
        assert get_typed_parameter_value("Location", root) == siteName()
        assert get_typed_parameter_value("JobStart", root)
        assert get_typed_parameter_value("JobEnd", root)
        assert get_typed_parameter_value("JobType", root) == updated_wf_commons.job_type

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

    def test_report_bk_prod_mcreconstruction_success(
        self, report_bk, job_path, wf_commons, bookkeeping_file, xml_summary_file
    ):
        """Test successful execution of ReportBookkeeping module."""
        wf_commons["steps"][0]["application_name"] = "Boole"
        wf_commons["job_type"] = "MCReconstruction"

        wf_commons["bookkeeping_lfns"] = [
            "/lhcb/LHCb/Collision16/SIM/00209455/0000/00209455_00001537_1",
        ]
        wf_commons["log_file_path"] = "/lhcb/LHCb/Collision16/LOG/00209455/0000/"
        wf_commons["production_output_data"] = [
            "/lhcb/LHCb/Collision16/SIM/00209455/0000/00209455_00001537_1",
        ]

        wf_commons["steps"][0]["start_time"] = time.time() - 1000

        wf_commons["steps"][0]["inputs"] = ["/lhcb/MC/2018/SIM/00212581/0000/00212581_00001446_1.sim"]
        wf_commons["steps"][0]["outputs"] = [
            {"outputDataName": "00209455_00001537_1", "outputDataType": "digi"},
        ]
        wf_commons["steps"][0]["application_log"] = "application.log"
        Path(wf_commons["steps"][0]["application_log"]).touch()
        Path(wf_commons["steps"][0]["outputs"][0]["outputDataName"]).touch()

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file

        WorkflowCommons(**wf_commons).save(job_path)

        report_bk.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        # Check if the XML report file is created
        assert Path(bookkeeping_file).exists(), "XML report file not created."

        # Validate the XML file
        tree = ET.parse(bookkeeping_file)
        root = tree.getroot()

        # Extract fields from the XML and perform further operations
        assert root.tag == "Job", "Root tag should be Job."
        assert root.attrib["ConfigName"] == updated_wf_commons.config_name
        assert root.attrib["ConfigVersion"] == updated_wf_commons.config_version
        assert root.attrib["Date"]
        assert root.attrib["Time"]

        assert get_typed_parameter_value("ProgramName", root) == updated_wf_commons.steps[0].application_name
        assert get_typed_parameter_value("ProgramVersion", root) == updated_wf_commons.steps[0].application_version
        assert get_typed_parameter_value("DiracVersion", root) == LHCbDIRAC.__version__
        assert get_typed_parameter_value("Name", root) == updated_wf_commons.steps[0].id
        assert float(get_typed_parameter_value("ExecTime", root)) > 1000
        assert get_typed_parameter_value("CPUTIME", root) == "0"

        assert get_typed_parameter_value("FirstEventNumber", root) == "1"
        assert get_typed_parameter_value("StatisticsRequested", root) == str(
            updated_wf_commons.steps[0].number_of_events
        )
        assert get_typed_parameter_value("NumberOfEvents", root) == str(xf_o.inputEventsTotal)

        assert get_typed_parameter_value("Production", root) == updated_wf_commons.production_id
        assert get_typed_parameter_value("DiracJobId", root) == str(updated_wf_commons.job_id)
        assert get_typed_parameter_value("Location", root) == siteName()
        assert get_typed_parameter_value("JobStart", root)
        assert get_typed_parameter_value("JobEnd", root)
        assert get_typed_parameter_value("JobType", root) == updated_wf_commons.job_type

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
        assert first_output_details["Name"] == updated_wf_commons.production_output_data[0]
        assert first_output_details["TypeName"] == "DIGI"
        assert first_output_details["Parameters"]["FileSize"] == "0"
        assert "CreationDate" in first_output_details["Parameters"]
        assert "MD5Sum" in first_output_details["Parameters"]
        assert "Guid" in first_output_details["Parameters"]

        assert len(output_files) == 1

        # Because we are using Gauss, sim conditions should be present too
        simulation_condition = root.find("SimulationCondition")
        assert simulation_condition is None, "SimulationCondition element should not be present."

    def test_report_bk_previousError_success(self, report_bk, job_path, wf_commons, bookkeeping_file):
        """Test previous command failure."""
        wf_commons["steps"][0]["application_name"] = "Gauss"
        wf_commons["steps"][0]["application_version"] = wf_commons["config_version"]
        wf_commons["job_type"] = "MCSimulation"
        wf_commons["step_status"] = StepStatus.Failed

        WorkflowCommons(**wf_commons).save(job_path)

        report_bk.execute(job_path)

        assert not Path(bookkeeping_file).exists()
