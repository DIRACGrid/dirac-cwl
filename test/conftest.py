"""Common pytest fixture used by test modules."""

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
