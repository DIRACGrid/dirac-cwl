"""
Tests for the job wrapper class.

This module tests the functionalities of the job wrapper.
"""

import os

import pytest

os.environ["DIRAC_PROTO_LOCAL"] = "1"

from dirac_cwl.core.exceptions import WorkflowProcessingException
from dirac_cwl.execution_hooks.core import ExecutionHooksBasePlugin


class TestJobWrapper:
    """Test the JobWrapper class."""

    def test_instantiation(self, sample_job, job_wrapper):
        """Test that ExecutionHooksBasePlugin can be instantiated directly with default behavior."""
        hook = ExecutionHooksBasePlugin()
        job_wrapper.execution_hooks_plugin = hook

        # Test default pre_process behavior
        command = ["cwltool", "--parallel", "task.cwl"]
        result = job_wrapper.pre_process(sample_job.task, None)
        assert result == command  # Should return command unchanged

        # Test default post_process behavior
        result = job_wrapper.post_process(0, "{}", "{}")  # Should not raise any exception
        assert result

        # Test default run_job behavior
        result = job_wrapper.run_job(sample_job)
        assert result

    def test_execute(self, job_type_testing, sample_job, mocker, monkeypatch, job_wrapper):
        """Test the execution of the preprocess and postprocess commands.

        The fixture "job_type_testing" is the class "JobTypeTestingPlugin".
        It uses the plugin class from the fixture, even though the commands will be overwritten.
        """
        from dirac_cwl.commands import PostProcessCommand, PreProcessCommand

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

        job_wrapper.pre_process(sample_job.task, None)
        execute_preprocess_mock.assert_called_once()
        execute_dualprocess_mock.assert_called_once()

        execute_dualprocess_mock.reset_mock()  # Reset the mock to be able to call "assert_called_once"

        job_wrapper.post_process(0, "{}", "{}")
        execute_postprocess_mock.assert_called_once()
        execute_dualprocess_mock.assert_called_once()

        # Test #2: The commands were set in the wrong processing step.
        #   Expected Result: The call raises "TypeError"
        plugin.preprocess_commands = [PostProcessCmd, DualProcessCmd]
        plugin.postprocess_commands = [PreProcessCmd, DualProcessCmd]

        with pytest.raises(TypeError):
            job_wrapper.pre_process(sample_job.task, None)

        with pytest.raises(TypeError):
            job_wrapper.post_process(0, "{}", "{}")

    def test_command_exception(self, job_type_testing, sample_job, mocker, monkeypatch, job_wrapper):
        """Test exception report when a command fails.

        The fixture "job_type_testing" is the class "JobTypeTestingPlugin".
        It uses the plugin class from the fixture, even though the commands will be overwritten.
        """
        from dirac_cwl.commands import PostProcessCommand, PreProcessCommand

        # Initialization and set the execute function to raise an exception
        class Command(PreProcessCommand, PostProcessCommand):
            def execute(job_path, **kwargs):
                raise NotImplementedError()

        plugin = job_type_testing()
        job_wrapper.execution_hooks_plugin = plugin

        plugin.preprocess_commands = [Command]
        plugin.postprocess_commands = [Command]

        # The processing steps should raise a "WorkflowProcessingException"
        with pytest.raises(WorkflowProcessingException):
            job_wrapper.pre_process(sample_job.task, None)

        with pytest.raises(WorkflowProcessingException):
            job_wrapper.post_process(0, "{}", "{}")
