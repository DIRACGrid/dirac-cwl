"""CLI interface to run a workflow as a production."""

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import typer
from cwl_utils.pack import pack
from cwl_utils.parser import load_document
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
    Workflow,
    WorkflowInputParameter,
    WorkflowStep,
)
from rich import print_json
from rich.console import Console
from schema_salad.exceptions import ValidationException

from dirac_cwl_proto.submission_models import (
    ProductionSubmissionModel,
    TransformationSubmissionModel,
)
from dirac_cwl_proto.transformation import (
    submit_transformation_router,
)

app = typer.Typer()
console = Console()


# -----------------------------------------------------------------------------
# dirac-cli commands
# -----------------------------------------------------------------------------


@app.command("submit")
def submit_production_client(
    task_path: str = typer.Argument(..., help="Path to the CWL file"),
    # Specific parameter for the purpose of the prototype
    local: Optional[bool] = typer.Option(True, help="Run the job locally instead of submitting it to the router"),
):
    """
    Correspond to the dirac-cli command to submit productions.

    This command will:
    - Validate the workflow
    - Start the production
    """
    os.environ["DIRAC_PROTO_LOCAL"] = "0"

    # Validate the workflow
    console.print("[blue]:information_source:[/blue] [bold]CLI:[/bold] Validating the production...")
    try:
        task = load_document(pack(task_path))
    except FileNotFoundError as ex:
        console.print(f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to load the task:\n{ex}")
        return typer.Exit(code=1)
    except ValidationException as ex:
        console.print(f"[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to validate the task:\n{ex}")
        return typer.Exit(code=1)
    console.print(f"\t[green]:heavy_check_mark:[/green] Task {task_path}")
    console.print("\t[green]:heavy_check_mark:[/green] Metadata")

    # Create the production
    production = ProductionSubmissionModel(task=task)
    console.print("[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Production validated.")

    # Submit the tranaformation
    console.print("[blue]:information_source:[/blue] [bold]CLI:[/bold] Submitting the production...")
    print_json(production.model_dump_json(indent=4))
    if not submit_production_router(production):
        console.print("[red]:heavy_multiplication_x:[/red] [bold]CLI:[/bold] Failed to run production.")
        return typer.Exit(code=1)
    console.print("[green]:heavy_check_mark:[/green] [bold]CLI:[/bold] Production done.")


# -----------------------------------------------------------------------------
# dirac-router commands
# -----------------------------------------------------------------------------


def submit_production_router(production: ProductionSubmissionModel) -> bool:
    """Submit a production to the router.

    :param production: The production to submit

    :return: True if the production was submitted successfully, False otherwise
    """
    logger = logging.getLogger("ProductionRouter")

    # Validate the transformation
    logger.info("Validating the production...")
    # Already validated by the pydantic model
    logger.info("Production validated!")

    # Split the production into transformations
    logger.info("Creating transformations from production...")
    transformations = _get_transformations(production)
    logger.info(f"{len(transformations)} transformations created!")

    # Submit the transformations
    logger.info("Submitting transformations...")
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(submit_transformation_router, transformations))

    return all(results)


# -----------------------------------------------------------------------------
# Production management
# -----------------------------------------------------------------------------


def _get_transformations(
    production: ProductionSubmissionModel,
) -> List[TransformationSubmissionModel]:
    """Create transformations from a given production.

    :param production: The production to create transformations from
    """
    # Create a subworkflow and a transformation for each step
    transformations = []

    for step in production.task.steps:
        step_task = _create_subworkflow(step, str(production.task.cwlVersion), production.task.inputs)

        transformations.append(
            TransformationSubmissionModel(
                task=step_task,
            )
        )
    return transformations


def _create_subworkflow(
    wf_step: WorkflowStep, cwlVersion: str, inputs: List[WorkflowInputParameter]
) -> Workflow | CommandLineTool | ExpressionTool:
    """Create a CWL file for a given step.

    If the step is a workflow, a new workflow is created.
    If the step is a command line tool, a new command line tool is created.

    :param wf_step: The step to create a CWL file for
    :param cwlVersion: The CWL version to use

    :return: The CWL subworkflow
    """
    new_workflow: Workflow | CommandLineTool
    if wf_step.run.class_ == "Workflow":
        # Handle nested workflows
        new_workflow = Workflow(
            cwlVersion=cwlVersion,
            inputs=wf_step.run.inputs,
            outputs=wf_step.run.outputs,
            steps=wf_step.run.steps,
            requirements=wf_step.run.requirements,
        )
    else:
        # Handle command line tools
        new_workflow = CommandLineTool(
            cwlVersion=cwlVersion,
            arguments=wf_step.run.arguments,
            baseCommand=wf_step.run.baseCommand,
            inputs=wf_step.run.inputs,
            outputs=wf_step.run.outputs,
            requirements=wf_step.run.requirements,
        )

    # Add the default value to the inputs if any
    for new_workflow_input in new_workflow.inputs:
        found_default = False

        if not new_workflow_input.id:
            continue

        new_workflow_input_name = new_workflow_input.id.split("#")[-1].split("/")[-1]
        for wf_step_in in wf_step.in_:
            # Skip if the input is not set: this should never happen
            if not wf_step_in.id:
                continue

            if new_workflow_input_name == wf_step_in.id.split("#")[-1].split("/")[-1]:
                # Find the source input from the original workflow
                for input in inputs:
                    # Skip if the input is not set: this should never happen
                    if not input.id:
                        continue

                    if input.id == wf_step_in.source:
                        new_workflow_input.default = input.default
                        found_default = True
                        break

            if found_default:
                break

    return new_workflow
