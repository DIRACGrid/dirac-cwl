"""
Tests for the core execution hooks plugins.

This module tests the built-in execution hooks plugins including the
QueryBased plugin implementation.
"""

import os
from pathlib import Path

import pytest
from DIRACCommon.Core.Utilities.ReturnValues import SErrorException

from dirac_cwl_proto.core.exceptions import WorkflowProcessingException
from dirac_cwl_proto.execution_hooks.plugins.core import (
    QueryBasedPlugin,
)


class TestQueryBasedPlugin:
    """Test the QueryBasedPlugin plugin."""

    def test_creation_and_parameters(self):
        """Test QueryBasedPlugin creation with default and custom parameters."""
        # Test default values
        plugin = QueryBasedPlugin()
        assert plugin.name() == "QueryBasedPlugin"
        assert plugin.query_root == "/grid/data"  # Default value
        assert plugin.site is None
        assert plugin.campaign is None
        assert plugin.data_type is None

        # Test custom parameters
        plugin = QueryBasedPlugin(query_root="/data", site="CERN", campaign="Run3", data_type="AOD")
        assert plugin.query_root == "/data"
        assert plugin.site == "CERN"
        assert plugin.campaign == "Run3"
        assert plugin.data_type == "AOD"

    def test_get_input_query_with_parameters(self):
        """Test get_input_query with query parameters."""
        plugin = QueryBasedPlugin(query_root="/data", campaign="Run3", site="CERN", data_type="AOD")

        result = plugin.get_input_query("test_input")

        # Should build path from query parameters (includes campaign/site/data_type and filename)
        expected = Path("/data/Run3/CERN/AOD/test_input")
        assert result == expected

    def test_get_input_query_partial_parameters(self):
        """Test get_input_query with partial parameters."""
        plugin = QueryBasedPlugin(
            query_root="/data",
            campaign="Run3",
            data_type="AOD",
            # No site
        )

        result = plugin.get_input_query("test_input")

        # Should build path from available parameters (includes campaign/data_type and filename)
        expected = Path("/data/Run3/AOD/test_input")
        assert result == expected

    def test_get_input_query_no_parameters(self):
        """Test get_input_query with no query parameters."""
        plugin = QueryBasedPlugin()

        result = plugin.get_input_query("test_input")

        # Should return a path under the default root when no parameters are set
        expected = Path("/grid/data/test_input")
        assert result == expected

    def test_get_input_query_default_root(self):
        """Test get_input_query with default root."""
        plugin = QueryBasedPlugin(campaign="Test")

        result = plugin.get_input_query("test_input")

        # Should use default "/grid/data" root with campaign
        expected = Path("/grid/data/Test/test_input")
        assert result == expected

    def test_get_input_query_with_kwargs(self):
        """Test get_input_query with additional kwargs."""
        plugin = QueryBasedPlugin(query_root="/data", campaign="Run3")

        # Additional kwargs should be available for custom implementations
        result = plugin.get_input_query("test_input", custom_param="value")

        # Base implementation should still work (includes campaign and filename)
        expected = Path("/data/Run3/test_input")
        assert result == expected

    def test_store_output(self):
        """Test store_output method."""
        os.environ["DIRAC_PROTO_LOCAL"] = "1"
        plugin = QueryBasedPlugin()

        # Since store_output expects a string path not a dict, let's test the actual signature
        # store_output(output_name: str, src_path: str) -> None

        # This should work since QueryBasedPlugin provides an output path
        try:
            plugin.store_output({"test_output": "/tmp/test_file.txt"})
        except SErrorException:
            # Expected since the file doesn't exist
            pass

    def test_pre_process_behavior(self):
        """Test that pre_process works correctly."""
        plugin = QueryBasedPlugin(campaign="Test", data_type="SIM")

        command = ["python", "script.py"]
        result = plugin.pre_process({}, None, Path("/tmp"), command)

        # Should return command unchanged
        assert result == command

    def test_post_process_behavior(self):
        """Test that post_process works correctly."""
        plugin = QueryBasedPlugin()

        # Should not raise exception
        plugin.post_process(Path("/tmp"))


class TestPluginIntegration:
    """Test integration between different core plugins."""

    def test_all_plugins_have_description(self):
        """Test that all core plugins have description set."""
        plugins = [QueryBasedPlugin]

        for plugin_class in plugins:
            assert hasattr(plugin_class, "description")
            assert isinstance(plugin_class.description, str)
            assert len(plugin_class.description) > 0

    def test_all_plugins_implement_interface(self):
        """Test that all core plugins implement the required interfaces."""
        plugins = [QueryBasedPlugin()]

        for plugin in plugins:
            # Test ExecutionHooksBasePlugin interface
            assert hasattr(plugin, "pre_process")
            assert hasattr(plugin, "post_process")

            # Test DataCatalogInterface interface
            assert hasattr(plugin, "get_input_query")
            assert hasattr(plugin, "store_output")

            # Test that methods are callable
            assert callable(plugin.pre_process)
            assert callable(plugin.post_process)
            assert callable(plugin.get_input_query)
            assert callable(plugin.store_output)

    def test_plugin_serialization_compatibility(self):
        """Test that all plugins can be serialized consistently."""
        plugins = [
            QueryBasedPlugin(campaign="Test"),
        ]

        for plugin in plugins:
            # Test dict serialization
            data = plugin.model_dump()
            assert isinstance(data, dict)

            # Test JSON schema generation
            schema = plugin.model_json_schema()
            assert isinstance(schema, dict)
            assert "properties" in schema


class TestPluginCommands:
    """Test plugin pre-processing and post-processing commands."""

    @pytest.fixture
    def job_type_testing(self):
        """Register a plugin with 1 preprocess and 1 postprocess command.

        Creates and registers "JobTypeTestingPlugin" with DownloadConfig and GroupOutputs commands,
        and returns its class.
        """
        from dirac_cwl_proto.commands.download_config import DownloadConfig
        from dirac_cwl_proto.commands.group_outputs import GroupOutputs
        from dirac_cwl_proto.execution_hooks.core import ExecutionHooksBasePlugin
        from dirac_cwl_proto.execution_hooks.registry import get_registry

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

    def test_from_registry(self, job_type_testing):
        """Test the initialization from the registry.

        The plugin "JobTypeTestingPlugin" was registered with 1 command on each step at the fixture "job_type_testing".
        The fixture "job_type_testing" is the class "JobTypeTestingPlugin".
        """
        from dirac_cwl_proto.execution_hooks.registry import get_registry

        # Get the job from the registry
        registry = get_registry()
        plugin_class = registry.get_plugin("JobTypeTestingPlugin")

        # It should have a found it and be the same class
        assert plugin_class is not None
        assert plugin_class == job_type_testing  # Comparing types

        plugin_instance = plugin_class()

        assert len(plugin_instance.preprocess_commands) == 1
        assert len(plugin_instance.postprocess_commands) == 1

    def test_from_hints(self, job_type_testing):
        """Test the initialization from a hint.

        The plugin "JobTypeTestingPlugin" was registered with 1 command on each step at the fixture "job_type_testing".
        The fixture "job_type_testing" is the class "JobTypeTestingPlugin".
        """
        from dirac_cwl_proto.execution_hooks.core import ExecutionHooksHint

        # Get the plugin from the hints
        hint = ExecutionHooksHint(hook_plugin="JobTypeTestingPlugin")
        plugin_from_hint = hint.to_runtime()  # Returns an instance of the plugin

        # They should be the same class and not None
        assert plugin_from_hint is not None
        assert isinstance(plugin_from_hint, job_type_testing)

        plugin_instance = job_type_testing()

        # The instance from the hints and registry should have the same commands
        assert len(plugin_from_hint.preprocess_commands) == 1
        assert plugin_from_hint.preprocess_commands[0] == plugin_instance.preprocess_commands[0]

        assert len(plugin_from_hint.postprocess_commands) == 1
        assert plugin_from_hint.postprocess_commands[0] == plugin_instance.postprocess_commands[0]

    def test_execute(self, job_type_testing, mocker, monkeypatch):
        """Test the execution of the preprocess and postprocess commands.

        The fixture "job_type_testing" is the class "JobTypeTestingPlugin".
        It uses the plugin class from the fixture, even though the commands will be overwritten.
        """
        from dirac_cwl_proto.commands import PostProcessCommand, PreProcessCommand

        # Initialization
        class PreProcessCmd(PreProcessCommand):
            def execute(job_path, **kwargs):
                return

        class PostProcessCmd(PostProcessCommand):
            def execute(job_path, **kwargs):
                return

        class DualProcessCmd(PreProcessCommand, PostProcessCommand):
            def execute(job_path, **kwargs):
                return

        plugin = job_type_testing()

        # Mock the "execute" commands to be able to spy them
        execute_preprocess_mock = mocker.MagicMock()
        execute_postprocess_mock = mocker.MagicMock()
        execute_dualprocess_mock = mocker.MagicMock()

        monkeypatch.setattr(PreProcessCmd, "execute", execute_preprocess_mock)
        monkeypatch.setattr(PostProcessCmd, "execute", execute_postprocess_mock)
        monkeypatch.setattr(DualProcessCmd, "execute", execute_dualprocess_mock)

        # Test #1: The commands were set in the correct processing step
        #   Expected Result: Everything works as expected
        plugin.preprocess_commands = [PreProcessCmd, DualProcessCmd]
        plugin.postprocess_commands = [PostProcessCmd, DualProcessCmd]

        plugin.pre_process("/fake/dir", None, "", ["fake", "command"])
        execute_preprocess_mock.assert_called_once()
        execute_dualprocess_mock.assert_called_once()

        execute_dualprocess_mock.reset_mock()  # Reset the mock to be able to call "assert_called_once"

        plugin.post_process("/fake/dir")
        execute_postprocess_mock.assert_called_once()
        execute_dualprocess_mock.assert_called_once()

        # Test #2: The commands were set in the wrong processing step.
        #   Expected Result: The call raises "TypeError"
        plugin.preprocess_commands = [PostProcessCmd, DualProcessCmd]
        plugin.postprocess_commands = [PreProcessCmd, DualProcessCmd]

        with pytest.raises(TypeError):
            plugin.pre_process("/fake/dir", None, "", ["fake", "command"])

        with pytest.raises(TypeError):
            plugin.post_process("/fake/dir")

    def test_command_exception(self, job_type_testing, mocker, monkeypatch):
        """Test exception report when a command fails.

        The fixture "job_type_testing" is the class "JobTypeTestingPlugin".
        It uses the plugin class from the fixture, even though the commands will be overwritten.
        """
        from dirac_cwl_proto.commands import PostProcessCommand, PreProcessCommand

        # Initialization
        class Command(PreProcessCommand, PostProcessCommand):
            def execute(job_path, **kwargs):
                return

        plugin = job_type_testing()

        # Set the execute function to raise an exception
        execute_mock = mocker.MagicMock()
        execute_mock.side_effect = NotImplementedError()

        monkeypatch.setattr(Command, "execute", execute_mock)

        plugin.preprocess_commands = [Command]
        plugin.postprocess_commands = [Command]

        # The processing steps should raise a "WorkflowProcessingException"
        with pytest.raises(WorkflowProcessingException):
            plugin.pre_process("/fake/dir", None, "", ["fake", "command"])

        with pytest.raises(WorkflowProcessingException):
            plugin.post_process("/fake/dir")
