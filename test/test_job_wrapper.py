"""
Tests for the job wrapper class.

This module tests the functionalities of the job wrapper.
"""

from pathlib import Path

import pytest

from dirac_cwl_proto.core.exceptions import WorkflowProcessingException
from dirac_cwl_proto.execution_hooks.core import ExecutionHooksBasePlugin
from dirac_cwl_proto.job.job_wrapper import JobWrapper


class TestJobWrapper:
    """Test the JobWrapper class."""

    def test_instantiation(self):
        """Test that ExecutionHooksBasePlugin can be instantiated directly with default behavior."""
        hook = ExecutionHooksBasePlugin()
        job_wrapper = JobWrapper()
        job_wrapper.execution_hooks_plugin = hook

        # Test default pre_process behavior
        command = ["echo", "hello"]
        result = job_wrapper._JobWrapper__pre_process_hooks({}, None, Path("/tmp"), command)
        assert result == command  # Should return command unchanged

        # Test default post_process behavior
        job_wrapper._JobWrapper__post_process_hooks(Path("/tmp"), exit_code=0)  # Should not raise any exception

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
        job_wrapper = JobWrapper()
        job_wrapper.execution_hooks_plugin = plugin

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

        job_wrapper._JobWrapper__pre_process_hooks("/fake/dir", None, "", ["fake", "command"])
        execute_preprocess_mock.assert_called_once()
        execute_dualprocess_mock.assert_called_once()

        execute_dualprocess_mock.reset_mock()  # Reset the mock to be able to call "assert_called_once"

        job_wrapper._JobWrapper__post_process_hooks("/fake/dir")
        execute_postprocess_mock.assert_called_once()
        execute_dualprocess_mock.assert_called_once()

        # Test #2: The commands were set in the wrong processing step.
        #   Expected Result: The call raises "TypeError"
        plugin.preprocess_commands = [PostProcessCmd, DualProcessCmd]
        plugin.postprocess_commands = [PreProcessCmd, DualProcessCmd]

        with pytest.raises(TypeError):
            job_wrapper._JobWrapper__pre_process_hooks("/fake/dir", None, "", ["fake", "command"])

        with pytest.raises(TypeError):
            job_wrapper._JobWrapper__post_process_hooks("/fake/dir")

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
        job_wrapper = JobWrapper()
        job_wrapper.execution_hooks_plugin = plugin

        # Set the execute function to raise an exception
        execute_mock = mocker.MagicMock()
        execute_mock.side_effect = NotImplementedError()

        monkeypatch.setattr(Command, "execute", execute_mock)

        plugin.preprocess_commands = [Command]
        plugin.postprocess_commands = [Command]

        # The processing steps should raise a "WorkflowProcessingException"
        with pytest.raises(WorkflowProcessingException):
            job_wrapper._JobWrapper__pre_process_hooks("/fake/dir", None, "", ["fake", "command"])

        with pytest.raises(WorkflowProcessingException):
            job_wrapper._JobWrapper__post_process_hooks("/fake/dir")
