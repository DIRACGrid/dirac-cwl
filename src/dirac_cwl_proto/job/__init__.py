"""CLI interface to run a workflow as a job."""

import logging
import os
import random
import subprocess
from pathlib import Path
from typing import Any

import typer
from cwl_utils.pack import pack
from cwl_utils.parser import load_document
from cwl_utils.parser.cwl_v1_2 import (
    File,
)
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile
from diracx.cli.utils import AsyncTyper
from rich import print_json
from rich.console import Console
from schema_salad.exceptions import ValidationException

from dirac_cwl_proto.job.submission_clients import (
    DIRACSubmissionClient,
    PrototypeSubmissionClient,
    SubmissionClient,
)
from dirac_cwl_proto.submission_models import (
    JobInputModel,
    JobModel,
    JobSubmissionModel,
)

app = AsyncTyper()
console = Console()


# -----------------------------------------------------------------------------
# dirac-cli commands
# -----------------------------------------------------------------------------
@app.async_command("submit")
async def submit_job_client(
    task_path: str = typer.Argument(..., help="Path to the CWL file"),
    parameter_path: list[str] | None = typer.Option(None, help="Path to the files containing the metadata"),
    # Specific parameter for the purpose of the prototype
    local: bool | None = typer.Option(True, help="Run the job locally instead of submitting it to the router"),
):
    """
    Correspond to the dirac-cli command to submit jobs.

    This command will:
    - Validate the workflow
    - Start the jobs
    """
    # Select submission strategy based on local flag
    submission_client: SubmissionClient = PrototypeSubmissionClient() if local else DIRACSubmissionClient()

    os.environ["DIRAC_PROTO_LOCAL"] = "0"

    # Validate the workflow
    console.print("[blue]:information_source:[/blue] [bold]CLI:[/bold] Validating the job(s)...")
    try:
        task = load_document(pack(task_path), baseuri=".")
    except FileNotFoundError as ex:
        console.print(f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to load the task:\n{ex}")
        return typer.Exit(code=1)
    except ValidationException as ex:
        console.print(f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to validate the task:\n{ex}")
        return typer.Exit(code=1)

    console.print(f"\t[green]:heavy_check_mark:[/green] Task {task_path}")
    console.print("\t[green]:heavy_check_mark:[/green] Hints")

    # Extract parameters if any
    parameters = []
    if parameter_path:
        for parameter_p in parameter_path:
            try:
                parameter = load_inputfile(parameter_p)
            except Exception as ex:
                console.print(
                    f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to validate the parameter:\n{ex}"
                )
                return typer.Exit(code=1)

            # Prepare files for the ISB
            isb_file_paths = prepare_input_sandbox(parameter)

            # Upload parameter sandbox
            sandbox_id = await submission_client.create_sandbox(isb_file_paths)

            parameters.append(
                JobInputModel(
                    sandbox=[sandbox_id] if sandbox_id else None,
                    cwl=parameter,
                )
            )
            console.print(f"\t[green]:heavy_check_mark:[/green] Parameter {parameter_p}")

    job = JobSubmissionModel(
        task=task,
        inputs=parameters,
    )
    console.print("[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Job(s) validated.")

    # Submit the job
    console.print("[blue]:information_source:[/blue] [bold]CLI:[/bold] Submitting the job(s)...")
    print_json(job.model_dump_json(indent=4))

    if not await submission_client.submit_job(job):
        console.print("[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to submit job(s).")
        return typer.Exit(code=1)


def validate_jobs(job: JobSubmissionModel) -> list[JobModel]:
    """
    Validate jobs.

    :param job: The task to execute

    :return: The list of jobs to execute
    """
    console.print("[blue]:information_source:[/blue] [bold]CLI:[/bold] Validating the job(s)...")
    # Initiate 1 job per parameter
    jobs = []
    if not job.inputs:
        jobs.append(
            JobModel(
                task=job.task,
            )
        )
    else:
        for parameter in job.inputs:
            jobs.append(
                JobModel(
                    task=job.task,
                    input=parameter,
                )
            )
    console.print("[green]:information_source:[/green] [bold]CLI:[/bold] Job(s) validated!")
    return jobs


def prepare_input_sandbox(input_data: dict[str, Any]) -> list[Path]:
    """
    Extract the files from the parameters.

    :param parameters: The parameters of the job

    :return: The list of files
    """
    # Get the files from the input data
    files = []
    for _, input_value in input_data.items():
        if isinstance(input_value, list):
            for item in input_value:
                if isinstance(item, File):
                    files.append(item)
        elif isinstance(input_value, File):
            files.append(input_value)

    files_path = []
    for file in files:
        # TODO: path is not the only attribute to consider, but so far it is the only one used
        if not file.location and not file.path:
            raise NotImplementedError("File path is not defined.")

        if file.path:
            file_path = Path(file.path.replace("file://", ""))
            files_path.append(file_path)

    return files_path


# -----------------------------------------------------------------------------
# dirac-router commands
# -----------------------------------------------------------------------------


def submit_job_router(job: JobSubmissionModel) -> bool:
    """
    Execute a job using the router.

    :param job: The task to execute

    :return: True if the job executed successfully, False otherwise
    """
    logger = logging.getLogger("JobRouter")

    os.environ["DIRAC_PROTO_LOCAL"] = "1"

    # Validate the jobs
    jobs = validate_jobs(job)

    # Execute the job locally
    logger.info("Executing jobs locally...")
    results = []

    for job in jobs:
        job_id = random.randint(1000, 9999)
        results.append(run_job(job_id, job, logger.getChild(f"job-{job_id}")))

    return all(results)


# -----------------------------------------------------------------------------
# Worker node execution
# -----------------------------------------------------------------------------


def run_job(job_id: int, job: JobModel, logger: logging.Logger) -> bool:
    """
    Run a single job by dumping it to JSON and executing the job_wrapper_template.py script.

    :param job: The job to execute
    :param logger: Logger instance for output

    :return: True if the job executed successfully, False otherwise
    """
    logger.info("Executing job locally:\n")
    print_json(job.model_dump_json(indent=4))

    # Dump job to a JSON file
    job_json_path = Path(f"job_{job_id}.json")
    with open(job_json_path, "w") as f:
        f.write(job.model_dump_json())

    # Run the job_wrapper_template.py script via bash command
    result = subprocess.run(
        [
            "python",
            "-m",
            "dirac_cwl_proto.job.job_wrapper_template",
            str(job_json_path),
        ],
        capture_output=True,
        text=True,
    )

    # Clean up the job JSON file
    job_json_path.unlink()

    # Log output
    if result.stdout:
        logger.info("STDOUT %s:\n%s", job_id, result.stdout)
    if result.stderr:
        logger.error("STDERR %s:\n%s", job_id, result.stderr)

    logger.info("Job execution completed.")
    return result.returncode == 0
