"""Common pytest fixture used by test modules."""

import shutil
from pathlib import Path

import pytest
from cwl_utils.parser.cwl_v1_2 import CommandLineTool

from dirac_cwl.submission_models import JobModel


@pytest.fixture
def job_type_testing():
    """Register a plugin with 1 preprocess and 1 postprocess command.

    Creates and registers "JobTypeTestingPlugin" with DownloadConfig and GroupOutputs commands,
    and returns its class.
    """
    from dirac_cwl.commands.download_config import DownloadConfig
    from dirac_cwl.commands.group_outputs import GroupOutputs
    from dirac_cwl.execution_hooks.core import ExecutionHooksBasePlugin
    from dirac_cwl.execution_hooks.registry import get_registry

    registry = get_registry()

    # Initialization
    class JobTypeTestingPlugin(ExecutionHooksBasePlugin):
        def __init__(self, **data):
            super().__init__(**data)

            self.preprocess_commands = [DownloadConfig]
            self.postprocess_commands = [GroupOutputs]

    # Add plugin to registry
    registry.register_plugin(JobTypeTestingPlugin)

    yield JobTypeTestingPlugin

    # Tear down
    registry._plugins.pop(JobTypeTestingPlugin.__name__)


@pytest.fixture
def sample_command_line_tool():
    """Create a sample CommandLineTool."""
    return CommandLineTool(
        id=".", inputs=[], outputs=[], requirements=[], cwlVersion="v1.2", baseCommand=["echo", "Hello World"]
    )


@pytest.fixture
def sample_job(sample_command_line_tool):
    """Create a sample JobModel."""
    return JobModel(task=sample_command_line_tool)


@pytest.fixture
def job_path():
    """Job Path Fixture."""
    path = Path(".").joinpath("examplepath")

    if path.exists():
        raise Exception("NON EMPTY DIRECTORY !!!")

    path.mkdir()

    yield path

    shutil.rmtree(path)


@pytest.fixture
def wf_commons():
    """Workflow commons dictionary fixture."""
    return {
        "job_id": 0,
        "job_type": "merge",
        "production_id": "123",
        "prod_job_id": "00000456",
        "inputs": [],
        "outputs": [],
        "config_name": "aConfigName",
        "config_version": "aConfigVersion",
        "steps": [
            {
                "id": "1",
                "name": "",
                "number": 1,
                "executable": "",
                "event_type": "123456789",
                "number_of_events": 100,
                "application_name": "someApp",
                "application_version": "v1r0",
                "inputs": [],
            }
        ],
    }


@pytest.fixture
def xml_summary_file(job_path, wf_commons):
    """XMLSummaryFile file path fixture."""
    path = Path(job_path).joinpath(
        f"summary{wf_commons['steps'][0]['application_name']}_{wf_commons['production_id']}_{wf_commons['prod_job_id']}_{wf_commons['steps'][0]['id']}.xml"
    )
    yield str(path)
    path.unlink(missing_ok=True)


@pytest.fixture
def request_file(job_path, wf_commons):
    """RequstDict file path fixture."""
    path = Path(job_path).joinpath(f"{wf_commons['production_id']}_{wf_commons['prod_job_id']}_request.json")
    yield str(path)
    path.unlink(missing_ok=True)


@pytest.fixture
def bookkeeping_file(job_path, wf_commons):
    """Bookkeeping report file fixture."""
    path = Path(job_path).joinpath(f"bookkeeping_{wf_commons['steps'][0]['id']}.xml")
    yield str(path)
    Path(path).unlink(missing_ok=True)
