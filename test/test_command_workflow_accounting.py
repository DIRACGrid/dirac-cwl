"""Test for the RegisterAccountingReport command class."""

from pathlib import Path
from textwrap import dedent

import pytest
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from pytest_mock import MockerFixture

from dirac_cwl.commands import RegisterAccountingReport
from dirac_cwl.commands.workflow_commons import StepStatus, WorkflowCommons


class TestRegisterAccountingReport:
    """Collection of tests for the RegisterAccountingReport command."""

    @pytest.fixture
    def accounting(self, mocker: MockerFixture, job_path):
        """Fixture for RegisterAccountingReport module."""
        command = RegisterAccountingReport()

        command.dsc = DataStoreClient()
        mocker.patch.object(command.dsc, "addRegister")

        yield command

        Path(job_path).joinpath("workflow_commons.json").unlink(missing_ok=True)

    # Test Scenarios
    def test_accounting_success(self, mocker: MockerFixture, job_path, accounting, wf_commons, xml_summary_file):
        """Test successful execution of RegisterAccountingReport module."""
        wf_commons["steps"][0]["application_name"] = "Gauss"
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

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["bk_id"] = "12345"
        wf_commons["steps"][0]["proc_pass"] = "Sim09m"
        wf_commons["steps"][0]["event_type"] = "23103003"

        WorkflowCommons(**wf_commons).save(job_path)

        accounting.execute(job_path)

        WorkflowCommons.load(job_path)

        # Make sure the dsc was called
        assert accounting.dsc.addRegister.assert_called_once

    def test_accounting_noApplicationName_fail(
        self, mocker: MockerFixture, job_path, accounting, wf_commons, xml_summary_file
    ):
        """Test RegisterAccountingReport when there is no application name in step commons."""
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

        wf_commons["steps"][0]["application_name"] = None
        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file

        WorkflowCommons(**wf_commons).save(job_path)

        accounting.execute(job_path)

        WorkflowCommons.load(job_path)

        # Make sure the dsc was not called
        assert accounting.dsc.addRegister.assert_not_called

    def test_accounting_incompleteData_success(
        self, mocker: MockerFixture, job_path, accounting, wf_commons, xml_summary_file
    ):
        """Test successful execution of RegisterAccountingReport module."""
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

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["application_name"] = "Gauss"

        WorkflowCommons(**wf_commons).save(job_path)

        accounting.execute(job_path)

        WorkflowCommons.load(job_path)

        # Make sure the dsc was not called
        assert accounting.dsc.addRegister.assert_not_called

    def test_accounting_previousError_fail(
        self, mocker: MockerFixture, job_path, accounting, wf_commons, xml_summary_file
    ):
        """Test RegisterAccountingReport with an intentional failure."""
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

        wf_commons["steps"][0]["xml_summary_path"] = xml_summary_file
        wf_commons["steps"][0]["application_name"] = "Gauss"
        wf_commons["steps"][0]["bk_id"] = "12345"
        wf_commons["steps"][0]["proc_pass"] = "Sim09m"
        wf_commons["steps"][0]["event_type"] = "23103003"
        wf_commons["step_status"] = StepStatus.Failed

        WorkflowCommons(**wf_commons).save(job_path)

        accounting.execute(job_path)

        WorkflowCommons.load(job_path)

        # Make sure the dsc was called
        assert accounting.dsc.addRegister.assert_called_once
