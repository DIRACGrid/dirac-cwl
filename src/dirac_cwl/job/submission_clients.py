"""
Submission client characteristics used in job client.

This module contains functions to manage job submission to the prototype, DIRAC, and DiracX backends.
It is not meant to be integrated to DiracX logic itself in the future.
"""

from abc import ABC, abstractmethod
from pathlib import Path

from diracx.api.jobs import create_sandbox
from diracx.client.aio import AsyncDiracClient
from rich.console import Console

from dirac_cwl.core.utility import get_lfns
from dirac_cwl.submission_models import JobModel, JobSubmissionModel

console = Console()


class SubmissionClient(ABC):
    """Abstract base class for job submission strategies."""

    @abstractmethod
    async def create_sandbox(self, isb_file_paths: list[Path]) -> str | None:
        """
        Upload parameter files to the sandbox store.

        :param isb_file_paths: List of input sandbox file paths
        :param parameter_path: Path to the parameter file
        :return: Sandbox PFN or None
        """
        pass

    @abstractmethod
    async def submit_job(self, job_submission: JobSubmissionModel) -> bool:
        """
        Submit a job to the backend.

        :param job_submission: Job submission model
        """
        pass


class PrototypeSubmissionClient(SubmissionClient):
    """Submission client for local/prototype execution."""

    async def create_sandbox(self, isb_file_paths: list[Path]) -> str | None:
        """
        Upload files to the local sandbox store.

        :param isb_file_paths: List of input sandbox file paths
        :param parameter_path: Path to the parameter file (not used in local mode)
        :return: Sandbox PFN or None
        """
        from dirac_cwl.mocks.sandbox import (
            create_sandbox,
        )

        if not isb_file_paths:
            return None

        return await create_sandbox(paths=isb_file_paths)

    async def submit_job(self, job_submission: JobSubmissionModel) -> bool:
        """
        Submit a job to the backend.

        :param job_submission: Job submission model
        """
        from dirac_cwl.job import submit_job_router

        result = submit_job_router(job_submission)
        if result:
            console.print("[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Job(s) done.")
        return result


class DIRACSubmissionClient(SubmissionClient):
    """Submission client for DIRAC/DiracX production execution."""

    async def create_sandbox(
        self,
        isb_file_paths: list[Path],
    ) -> str | None:
        """
        Upload parameter files to the sandbox store.

        :param isb_file_paths: List of input sandbox file paths
        :return: Sandbox PFN or None
        """
        return await create_sandbox(isb_file_paths)

    async def submit_job(self, job_submission: JobSubmissionModel) -> bool:
        """
        Submit a job to the backend.

        :param job_submission: Job submission model
        """
        from dirac_cwl.job import validate_jobs

        jdls = []
        job_submission_path = Path("job.json")
        for job in validate_jobs(job_submission):
            # Dump the job model to a file
            with open(job_submission_path, "w") as f:
                f.write(job.model_dump_json())

            # Convert job.json to jdl
            console.print("\t\t[blue]:information_source:[/blue] [bold]CLI:[/bold] Converting job model to jdl...")
            sandbox_id = await create_sandbox([job_submission_path])
            job_submission_path.unlink()

            jdl = self.convert_to_jdl(job, sandbox_id)
            jdls.append(jdl)

        console.print("\t\t[blue]:information_source:[/blue] [bold]CLI:[/bold] Call diracx: jobs/jdl router...")

        async with AsyncDiracClient() as api:
            jdl_jobs = await api.jobs.submit_jdl_jobs(jdls)

        console.print(
            f"\t\t[green]:information_source:[/green] [bold]CLI:[/bold] Inserted {len(jdl_jobs)} jobs with ids:  \
            {','.join(map(str, (jdl_job.job_id for jdl_job in jdl_jobs)))}"
        )
        return True

    def convert_to_jdl(self, job: JobModel, sandbox_pfn: str) -> str:
        """Convert job model to jdl.

        .. deprecated::
            JDL conversion is now handled by ``cwl_to_jdl()`` in diracx-logic.
            This method will be removed once the DIRACSubmissionClient is
            updated to use the new ``POST /api/jobs/`` CWL endpoint.
        """
        raise NotImplementedError(
            "JDL conversion has moved to diracx-logic (cwl_to_jdl). "
            "Use the POST /api/jobs/ endpoint instead."
        )
