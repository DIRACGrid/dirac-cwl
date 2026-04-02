"""
Tests for the job wrapper class.

This module tests the functionalities of the job wrapper.
"""

import os
from random import randint
from shutil import rmtree

import pytest

os.environ["DIRAC_PROTO_LOCAL"] = "1"

from dirac_cwl.core.exceptions import WorkflowProcessingException
from dirac_cwl.job.job_report import JobMinorStatus, JobStatus
from dirac_cwl.job.job_wrapper import JobWrapper
from dirac_cwl.mocks.status import STATUS_DIR


class TestJobWrapper:
    """Test the JobWrapper class."""

    @pytest.mark.asyncio
    async def test_instantiation(self, sample_job):
        """Test that JobWrapper can run with no commands."""
        job_wrapper = JobWrapper(job_id=0)

        # Test default pre_process behavior (no commands)
        command = ["cwltool", "--parallel", "task.cwl"]
        result = await job_wrapper.pre_process(sample_job.task, None)
        assert result == command

        # Test default post_process behavior (no commands)
        result = await job_wrapper.post_process(0, "{}", "{}")
        assert result

        # Test default run_job behavior
        result = await job_wrapper.run_job(sample_job)
        assert result

    @pytest.mark.asyncio
    async def test_execute(self, sample_job, mocker):
        """Test the execution of pre/post-process commands via the command lists."""
        from dirac_cwl.commands import PostProcessCommand, PreProcessCommand

        class PreProcessCmd(PreProcessCommand):
            async def execute(self, job_path, **kwargs):
                return

        class PostProcessCmd(PostProcessCommand):
            async def execute(self, job_path, **kwargs):
                return

        job_wrapper = JobWrapper(job_id=0)

        pre_cmd = PreProcessCmd()
        post_cmd = PostProcessCmd()

        mocker.patch.object(pre_cmd, "execute", new_callable=mocker.AsyncMock)
        mocker.patch.object(post_cmd, "execute", new_callable=mocker.AsyncMock)

        job_wrapper._preprocess_commands = [pre_cmd]
        job_wrapper._postprocess_commands = [post_cmd]

        await job_wrapper.pre_process(sample_job.task, None)
        pre_cmd.execute.assert_called_once()

        await job_wrapper.post_process(0, "{}", "{}")
        post_cmd.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_command_exception(self, sample_job):
        """Test exception report when a command fails."""
        from dirac_cwl.commands import PostProcessCommand, PreProcessCommand

        class FailingPreCmd(PreProcessCommand):
            async def execute(self, job_path, **kwargs):
                raise NotImplementedError()

        class FailingPostCmd(PostProcessCommand):
            async def execute(self, job_path, **kwargs):
                raise NotImplementedError()

        job_wrapper = JobWrapper(job_id=0)

        job_wrapper._preprocess_commands = [FailingPreCmd()]
        with pytest.raises(WorkflowProcessingException):
            await job_wrapper.pre_process(sample_job.task, None)

        job_wrapper._postprocess_commands = [FailingPostCmd()]
        with pytest.raises(WorkflowProcessingException):
            await job_wrapper.post_process(0, "{}", "{}")

    @pytest.mark.asyncio
    async def test_job_status(self):
        """Test the job status methods of the job report work as intended."""
        job_id = randint(0, 9999)
        job_wrapper = JobWrapper(job_id)
        file_path = STATUS_DIR / f"status_{job_id}"
        assert len(job_wrapper._job_report.job_status_info) == 1  # One status expected for initialization
        assert not file_path.exists()

        job_wrapper._job_report.set_job_status(status=JobStatus.RUNNING, minor_status=JobMinorStatus.APPLICATION)
        assert len(job_wrapper._job_report.job_status_info) == 2
        assert not file_path.exists()

        job_wrapper._job_report.set_job_status(application_status="Test")
        assert len(job_wrapper._job_report.job_status_info) == 3
        assert not file_path.exists()

        await job_wrapper._job_report.commit()
        assert file_path.exists()
        with open(file_path) as f:
            content = f.read()
            assert JobStatus.RUNNING in content
            assert JobMinorStatus.APPLICATION in content
            assert "Test" in content
        rmtree(STATUS_DIR)

    @pytest.mark.asyncio
    async def test_run_job_reports(self, sample_job):
        """Test job statuses are reported in the job wrapper."""
        job_id = randint(0, 9999)
        job_wrapper = JobWrapper(job_id)
        success = await job_wrapper.run_job(sample_job)
        assert success
        # Status info only stays accumulated for local testing, status info is emptied when committing to diracx
        assert len(job_wrapper._job_report.job_status_info) > 0
        assert (STATUS_DIR / f"status_{job_id}").exists()
        rmtree(STATUS_DIR)
