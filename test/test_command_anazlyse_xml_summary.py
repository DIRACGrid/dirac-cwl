"""Test for the AnalyseXmlSummary command class."""

from pathlib import Path
from textwrap import dedent

import pytest
from DIRACCommon.Core.Utilities.ReturnValues import S_OK
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary
from pytest_mock import MockerFixture

from dirac_cwl.commands import AnalyseXmlSummary
from dirac_cwl.commands.workflow_commons import StepStatus, WorkflowCommons
from dirac_cwl.core.exceptions import WorkflowProcessingException


class TestAnalyseXmlSummary:
    """Collection of tests for the AnalyseXmlSummary command."""

    @pytest.fixture
    def axlf(self, mocker: MockerFixture, job_path):
        """Fixture for AnalyseXmlSummary module."""
        yield AnalyseXmlSummary()

        Path(job_path).joinpath("workflow_commons.json").unlink(missing_ok=True)

    # Test scenarios
    def test_analyseXMLSummary_basic_success(self, mocker: MockerFixture, axlf, job_path, wf_commons, xml_summary_file):
        """Test basic success scenario."""
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus", return_value=S_OK()
        )

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        WorkflowCommons(**wf_commons).save(job_path)

        axlf.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        mock_setApplicationStatus.assert_called_once()
        assert updated_wf_commons.file_report.statusDict == {}

    def test_analyseXMLSummary_previousError_success(
        self, mocker: MockerFixture, axlf, job_path, wf_commons, xml_summary_file
    ):
        """Test success scenario with previous error: stepStatus = S_ERROR()."""
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus", return_value=S_OK()
        )

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["step_status"] = StepStatus.Failed
        wf_commons["steps"][0]["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        WorkflowCommons(**wf_commons).save(job_path)

        axlf.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        mock_setApplicationStatus.assert_not_called()
        assert updated_wf_commons.file_report.statusDict == {}

    def test_analyseXMLSummary_badInput_success(
        self, mocker: MockerFixture, axlf, job_path, wf_commons, xml_summary_file
    ):
        """Test success scenario with part and fail input not part of the input data list."""
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus", return_value=S_OK()
        )

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        WorkflowCommons(**wf_commons).save(job_path)

        axlf.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        mock_setApplicationStatus.assert_called_once()
        assert updated_wf_commons.file_report.statusDict == {}

    def test_analyseXMLSummary_partInput_success(
        self, mocker: MockerFixture, axlf, job_path, wf_commons, xml_summary_file
    ):
        """Test success scenario with part input part of the input data list."""
        # Input is 'part' and is part of the input data list but the number of events is not -1
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus", return_value=S_OK()
        )

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
        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["inputs"] = ["00012478_00000532_1.sim"]
        wf_commons["steps"][0]["number_of_events"] = 1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        WorkflowCommons(**wf_commons).save(job_path)

        axlf.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        mock_setApplicationStatus.assert_called_once()
        assert updated_wf_commons.file_report.statusDict == {}

    def test_analyseXMLSummary_notSuccess_fail(
        self, mocker: MockerFixture, axlf, job_path, wf_commons, xml_summary_file
    ):
        """Test failure scenario with success=False."""
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus", return_value=S_OK()
        )

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)

        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["number_of_events"] = -1

        assert xf_o.success == "False"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        WorkflowCommons(**wf_commons).save(job_path)

        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        mock_setApplicationStatus.assert_called_once()
        assert updated_wf_commons.file_report.statusDict == {}

    def test_analyseXMLSummary_badStep_fail(self, mocker: MockerFixture, axlf, job_path, wf_commons, xml_summary_file):
        """Test failure scenario with step != finalize."""
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus", return_value=S_OK()
        )

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "execute"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        WorkflowCommons(**wf_commons).save(job_path)

        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        mock_setApplicationStatus.assert_called_once()
        assert updated_wf_commons.file_report.statusDict == {}

    def test_analyseXMLSummary_badOutput_fail(
        self, mocker: MockerFixture, axlf, job_path, wf_commons, xml_summary_file
    ):
        """Test failure scenario with output status != full."""
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus", return_value=S_OK()
        )

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert not xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        WorkflowCommons(**wf_commons).save(job_path)

        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        mock_setApplicationStatus.assert_called_once()
        assert updated_wf_commons.file_report.statusDict == {}

    def test_analyseXMLSummary_badInput_fail(self, mocker: MockerFixture, axlf, job_path, wf_commons, xml_summary_file):
        """Test failure scenario with input status = mult."""
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus", return_value=S_OK()
        )

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        WorkflowCommons(**wf_commons).save(job_path)

        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        mock_setApplicationStatus.assert_called_once()
        assert updated_wf_commons.file_report.statusDict == {}

    def test_analyseXMLSummary_badInput2_fail(
        self, mocker: MockerFixture, axlf, job_path, wf_commons, xml_summary_file
    ):
        """Test failure scenario with an unknown input status (weoweo)."""
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus", return_value=S_OK()
        )

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert xf_o.inputFileStats["other"]

        WorkflowCommons(**wf_commons).save(job_path)

        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        mock_setApplicationStatus.assert_called_once()
        assert updated_wf_commons.file_report.statusDict == {}

    def test_analyseXMLSummary_badInput3_fail(
        self, mocker: MockerFixture, axlf, job_path, wf_commons, xml_summary_file
    ):
        """Test failure scenario with input status = fail."""
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus", return_value=S_OK()
        )

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["inputs"] = ["00012478_00000532_1.sim"]
        wf_commons["steps"][0]["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        WorkflowCommons(**wf_commons).save(job_path)

        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        mock_setApplicationStatus.assert_called_once()
        assert updated_wf_commons.file_report.statusDict == {"00012478_00000532_1.sim": "Problematic"}

    def test_analyseXMLSummary_badInput4_fail(
        self, mocker: MockerFixture, axlf, job_path, wf_commons, xml_summary_file
    ):
        """Test failure scenario with input status = part."""
        # Input is 'part' and is part of the input data list but the number of events is -1 (by default)
        mock_setApplicationStatus = mocker.patch(
            "DIRAC.WorkloadManagementSystem.Client.JobReport.JobReport.setApplicationStatus", return_value=S_OK()
        )

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

        with open(xml_summary_file, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xf_o = XMLSummary(xml_summary_file)

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["inputs"] = ["00012478_00000532_1.sim"]
        wf_commons["steps"][0]["number_of_events"] = -1

        assert xf_o.success == "True"
        assert xf_o.step == "finalize"
        assert xf_o._outputsOK()
        assert not xf_o.inputFileStats["mult"]
        assert not xf_o.inputFileStats["other"]

        WorkflowCommons(**wf_commons).save(job_path)

        with pytest.raises(WorkflowProcessingException):
            axlf.execute(job_path)

        updated_wf_commons = WorkflowCommons.load(job_path)

        mock_setApplicationStatus.assert_called_once()
        assert updated_wf_commons.file_report.statusDict == {"00012478_00000532_1.sim": "Problematic"}
