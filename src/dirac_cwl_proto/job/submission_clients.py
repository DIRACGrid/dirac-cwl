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

from dirac_cwl_proto.core.utility import get_lfns
from dirac_cwl_proto.execution_hooks import SchedulingHint
from dirac_cwl_proto.submission_models import JobModel, JobSubmissionModel

console = Console()


class SubmissionClient(ABC):
    """Abstract base class for job submission strategies."""

    @abstractmethod
    async def upload_sandbox(self, isb_file_paths: list[Path]) -> str | None:
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

    async def upload_sandbox(self, isb_file_paths: list[Path]) -> str | None:
        """
        Upload files to the local sandbox store.

        :param isb_file_paths: List of input sandbox file paths
        :param parameter_path: Path to the parameter file (not used in local mode)
        :return: Sandbox PFN or None
        """
        from dirac_cwl_proto.data_management_mocks.sandbox import (
            upload_sandbox,
        )

        if not isb_file_paths:
            return None

        Path("sandboxstore").mkdir(exist_ok=True)
        return upload_sandbox(paths=isb_file_paths)

    async def submit_job(self, job_submission: JobSubmissionModel) -> bool:
        """
        Submit a job to the backend.

        :param job_submission: Job submission model
        """
        from dirac_cwl_proto.job import submit_job_router

        result = submit_job_router(job_submission)
        if result:
            console.print("[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Job(s) done.")
        return result


class DIRACSubmissionClient(SubmissionClient):
    """Submission client for DIRAC/DiracX production execution."""

    async def upload_sandbox(
        self,
        isb_file_paths: list[Path],
    ) -> str | None:
        """
        Upload parameter files to the sandbox store.

        :param isb_file_paths: List of input sandbox file paths
        :param parameter_path: Path to the parameter file
        :return: Sandbox PFN or None
        """
        # Modify the location of the files to point to the future location on the worker node
        modified_paths = [Path(p.name) for p in isb_file_paths]
        return await create_sandbox(modified_paths)

    async def submit_job(self, job_submission: JobSubmissionModel) -> bool:
        """
        Submit a job to the backend.

        :param job_submission: Job submission model
        """
        from dirac_cwl_proto.job import validate_jobs

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
        """
        Convert job model to jdl.

        :param job: The task to execute
        :param sandbox_pfn: The sandbox PFN
        :return: JDL string
        """
        jdl_lines = []
        jdl_lines.append("Executable = dirac-cwl-exec;")
        jdl_lines.append("Arguments = job.json;")

        if job.task.requirements and job.task.requirements[0].coresMin:
            jdl_lines.append(f"NumberOfProcessors = {job.task.requirements[0].coresMin};")

        jdl_lines.append("JobName = test;")
        jdl_lines.append("OutputSandbox = {std.out, std.err};")

        job_scheduling = SchedulingHint.from_cwl(job.task)
        if job_scheduling.priority:
            jdl_lines.append(f"Priority = {job_scheduling.priority};")

        if job_scheduling.sites:
            jdl_lines.append(f"Site = {job_scheduling.sites};")

        jdl_lines.append(f"InputSandbox = {sandbox_pfn};")
        if job.input:
            formatted_lfns = []
            lfns_list = get_lfns(job.input.cwl).values()
            for lfns in lfns_list:
                for lfn in lfns:
                    formatted_lfns.append(str(lfn).replace("lfn:", "LFN:", 1))

            lfns_str = ", ".join(formatted_lfns)
            if lfns_str:
                jdl_lines.append(f"InputData = {lfns_str};")

        return "\n".join(jdl_lines)
