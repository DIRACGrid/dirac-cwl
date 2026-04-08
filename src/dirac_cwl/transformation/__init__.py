"""CLI interface to run a workflow as a transformation."""

import glob
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import typer
from cwl_utils.pack import pack
from cwl_utils.parser import load_document
from cwl_utils.parser.cwl_v1_2 import File
from cwl_utils.parser.utils import load_inputfile
from rich import print_json
from rich.console import Console
from schema_salad.exceptions import ValidationException

from dirac_cwl.execution_hooks import (
    TransformationExecutionHooksHint,
)
from dirac_cwl.job import submit_job_router
from dirac_cwl.submission_models import (
    JobInputModel,
    JobSubmissionModel,
    TransformationSubmissionModel,
)

app = typer.Typer()
console = Console()


# -----------------------------------------------------------------------------
# dirac-cli commands
# -----------------------------------------------------------------------------


def _parse_chunk(chunk_str: str) -> tuple[str, int]:
    """Parse a --chunk value of the form PARAM=SIZE.

    :param chunk_str: The chunk string to parse.
    :return: Tuple of (parameter name, chunk size).
    :raises typer.BadParameter: If the format is invalid or size is not a positive integer.
    """
    if "=" not in chunk_str:
        raise typer.BadParameter(f"Invalid --chunk format '{chunk_str}'. Expected PARAM=SIZE (e.g., input-data=3).")
    param, size_str = chunk_str.split("=", 1)
    if not param:
        raise typer.BadParameter("Parameter name cannot be empty in --chunk.")
    try:
        size = int(size_str)
    except ValueError as err:
        raise typer.BadParameter(f"Chunk size must be an integer, got '{size_str}'.") from err
    if size <= 0:
        raise typer.BadParameter(f"Chunk size must be > 0, got {size}.")
    return param, size


@app.command("submit")
def submit_transformation_client(
    task_path: str = typer.Argument(..., help="Path to the CWL file"),
    inputs_file: str | None = typer.Option(None, help="Path to the CWL inputs file"),
    chunk: str | None = typer.Option(None, help="Split an array input into jobs: PARAM=SIZE (e.g., input-data=3)"),
    # Specific parameter for the purpose of the prototype
    local: Optional[bool] = typer.Option(True, help="Run the jobs locally instead of submitting them to the router"),
):
    """
    Correspond to the dirac-cli command to submit transformations.

    This command will:
    - Validate the workflow
    - Start the transformation
    """
    os.environ["DIRAC_PROTO_LOCAL"] = "0"

    # --chunk and --inputs-file must be used together
    if chunk and not inputs_file:
        console.print("[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] --chunk requires --inputs-file.")
        return typer.Exit(code=1)
    if inputs_file and not chunk:
        console.print("[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] --inputs-file requires --chunk.")
        return typer.Exit(code=1)

    # Validate the workflow
    console.print("[blue]:information_source:[/blue] [bold]CLI:[/bold] Validating the transformation...")
    try:
        task = load_document(pack(task_path))

        # Warn if the hint already has input_data — CLI overrides it
        existing_hint = TransformationExecutionHooksHint.from_cwl(task)
        if inputs_file and existing_hint.input_data:
            console.print(
                "[yellow]:warning:[/yellow] [bold]CLI:[/bold] "
                "The workflow hint already contains input_data. "
                "Overriding with --inputs-file/--chunk values."
            )

        # Load and validate inputs, inject into hint
        if inputs_file and chunk:
            all_inputs = load_inputfile(task.cwlVersion, inputs_file)
            chunk_param, chunk_size = _parse_chunk(chunk)

            # Validate the chunk parameter exists and is a list
            if chunk_param not in all_inputs:
                console.print(
                    f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] "
                    f"Parameter '{chunk_param}' not found in inputs file. "
                    f"Available parameters: {list(all_inputs.keys())}"
                )
                return typer.Exit(code=1)
            if not isinstance(all_inputs[chunk_param], list):
                console.print(
                    f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] "
                    f"Parameter '{chunk_param}' must be an array type for --chunk, "
                    f"got {type(all_inputs[chunk_param]).__name__}."
                )
                return typer.Exit(code=1)

            input_data = {chunk_param: all_inputs[chunk_param]}

            # Inject input_data and group_size into the task's ExecutionHooks hint
            hint_update = TransformationExecutionHooksHint(group_size=chunk_size, input_data=input_data)
            TransformationExecutionHooksHint.update_cwl(task, hint_update)

    except FileNotFoundError as ex:
        console.print(f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to load the task:\n{ex}")
        return typer.Exit(code=1)
    except ValidationException as ex:
        console.print(f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to validate the task:\n{ex}")
        return typer.Exit(code=1)
    console.print(f"\t[green]:heavy_check_mark:[/green] Task {task_path}")

    transformation = TransformationSubmissionModel(task=task)
    console.print("[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Transformation validated.")

    # Submit the transformation
    console.print("[blue]:information_source:[/blue] [bold]CLI:[/bold] Submitting the transformation...")
    print_json(transformation.model_dump_json(indent=4))
    if not submit_transformation_router(transformation):
        console.print("[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to run transformation.")
        return typer.Exit(code=1)
    console.print("[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Transformation done.")


# -----------------------------------------------------------------------------
# dirac-router commands
# -----------------------------------------------------------------------------


def submit_transformation_router(transformation: TransformationSubmissionModel) -> bool:
    """Execute a transformation using the router.

    If the transformation is waiting for an input from another transformation,
    it will wait for the input to be available in the "bookkeeping".

    :param transformation: The transformation to start.
    :return: True if the transformation is executed successfully, False otherwise.
    """
    logger = logging.getLogger("TransformationRouter")

    # Validate the transformation
    logger.info("Validating the transformation...")
    # Already validated by the pydantic model
    logger.info("Transformation validated!")

    # Check if the transformation is waiting for an input
    # - if there is no execution_hooks, the transformation is not waiting for an input and can go on
    # - if there is execution_hooks, the transformation is waiting for an input
    job_model_params = []

    try:
        transformation_execution_hooks = TransformationExecutionHooksHint.from_cwl(transformation.task)
    except Exception as exc:
        raise ValueError(f"Invalid DIRAC hints:\n{exc}") from exc

    # Inputs from static input_data (populated by --chunk on the client)
    if transformation_execution_hooks.input_data:
        if transformation_execution_hooks.configuration:
            raise ValueError(
                "Cannot specify both static input_data and dynamic input query (configuration). "
                "Use --chunk/--inputs-file for standalone transformations with known files. "
                "Use configuration for transformations that discover inputs from upstream outputs."
            )

        group_size = transformation_execution_hooks.group_size or 1
        for param_name, file_list in transformation_execution_hooks.input_data.items():
            nb_files = len(file_list)
            nb_groups = (nb_files + group_size - 1) // group_size
            logger.info(
                "Chunking '%s': %s files into %s jobs (group_size=%s)",
                param_name,
                nb_files,
                nb_groups,
                group_size,
            )

            for i, start in enumerate(range(0, nb_files, group_size)):
                files_chunk = file_list[start : start + group_size]
                logger.info("Group %i files: %s", i + 1, files_chunk)
                job_model_params.append(JobInputModel(sandbox=None, cwl={param_name: files_chunk}))

    # Inputs from DataCatalog/Bookkeeping service (dynamic query)
    # NOTE: group_size is required here to distinguish "configuration as plugin overrides"
    # (e.g. num_points: 2000) from "configuration as query params" (e.g. query_root, campaign).
    # Without group_size, there's no grouping to do and no reason to query for input files.
    # This will be cleaned up when input_query becomes the explicit trigger (#69).
    elif transformation_execution_hooks.configuration and transformation_execution_hooks.group_size:
        group_size = transformation_execution_hooks.group_size

        # Get the metadata class
        transformation_metadata = transformation_execution_hooks.to_runtime(transformation)

        # Build the input cwl for the jobs to submit
        logger.info("Getting the input data for the transformation...")
        input_query = transformation_metadata.get_input_query()
        if not input_query:
            raise RuntimeError("Input query not found.")

        # Wait for the input to be available
        logger.info("\t- Waiting for input data...")
        logger.debug("\t\t- Query: %s", input_query)
        logger.debug("\t\t- Group Size: %s", group_size)

        while not (inputs := _get_inputs(input_query, group_size)):
            logger.debug("\t\t- Result: %s", inputs)
            time.sleep(5)

        logger.info("\t- Input data available.")

        # Get the JobModelParameter for each input
        input_data_dict = {"input-data": inputs}
        job_model_params = _generate_job_model_parameter(input_data_dict)
        logger.info("Input data for the transformation retrieved!")

    logger.info("Building the jobs...")
    jobs = JobSubmissionModel(
        task=transformation.task,
        inputs=job_model_params,
    )
    logger.info("Jobs built!")

    logger.info("Submitting jobs...")
    return submit_job_router(jobs)


# -----------------------------------------------------------------------------
# Transformation management
# -----------------------------------------------------------------------------


def _get_inputs(input_query: Path | list[Path], group_size: int) -> List[List[str]]:
    """Get the input data from the input query.

    :param input_query: The input query to get the input data
    :param group_size: The number of jobs to group together in a transformation
    :return: A list of lists of paths to the input data, each inner list has length group_size
    """
    # TODO: how do we know whether a given input has already been processed?

    # Retrieve all input paths matching the query
    if isinstance(input_query, Path):
        input_paths = glob.glob(str(input_query / "*"), root_dir="filecatalog")
    else:
        input_paths = []
        for query in input_query:
            input_paths.extend(glob.glob(str(query / "*"), root_dir="filecatalog"))
    len_input_paths = len(input_paths)

    # Ensure there are enough inputs to form at least one group
    if len_input_paths < group_size:
        return []

    # Calculate the number of full groups
    num_full_groups = len_input_paths // group_size

    # Group the input paths into lists of size group_size
    input_groups = [input_paths[i * group_size : (i + 1) * group_size] for i in range(num_full_groups)]

    return input_groups


def _generate_job_model_parameter(
    input_data_dict: Dict[str, List[List[str]]],
) -> List[JobInputModel]:
    """Generate job model parameters from input data provided."""
    job_model_params = []

    input_names = list(input_data_dict.keys())
    input_data_lists = [input_data_dict[input_name] for input_name in input_names]
    grouped_input_data = [dict(zip(input_names, elements)) for elements in zip(*input_data_lists)]
    for group in grouped_input_data:
        cwl_inputs = {}
        for input_name, input_data in group.items():
            cwl_inputs[input_name] = [File(location=str(Path("lfn:") / path)) for path in input_data]

        job_model_params.append(JobInputModel(sandbox=None, cwl=cwl_inputs))

    return job_model_params
