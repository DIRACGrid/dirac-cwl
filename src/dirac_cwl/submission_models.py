"""
Enhanced submission models for DIRAC CWL integration.

This module provides improved submission models with proper separation of concerns,
modern Python typing, and comprehensive numpydoc documentation.
"""

from __future__ import annotations

from typing import Any

from cwl_utils.parser import WorkflowStep, save
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
    ResourceRequirement,
    Workflow,
)
from pydantic import BaseModel, ConfigDict, field_serializer, model_validator

from dirac_cwl_proto.execution_hooks import (
    ExecutionHooksHint,
    SchedulingHint,
    TransformationExecutionHooksHint,
)

# -----------------------------------------------------------------------------
# Job models
# -----------------------------------------------------------------------------


class JobInputModel(BaseModel):
    """Input data and sandbox files for a job execution."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    sandbox: list[str] | None
    cwl: dict[str, Any]

    @field_serializer("cwl")
    def serialize_cwl(self, value):
        return save(value)


class JobSubmissionModel(BaseModel):
    """Job definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: CommandLineTool | Workflow | ExpressionTool
    parameters: list[JobInputModel] | None = None
    scheduling: SchedulingHint
    execution_hooks: ExecutionHooksHint

    @model_validator(mode="before")
    def validate_job(cls, values):
        task = values.get("task")

        # Validate Resource Requirement values of CWLObject, will raise ValueError if needed.
        validate_resource_requirements(task)

        return values

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (CommandLineTool, Workflow, ExpressionTool)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


# -----------------------------------------------------------------------------
# Transformation models
# -----------------------------------------------------------------------------


class TransformationSubmissionModel(BaseModel):
    """Transformation definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: CommandLineTool | Workflow | ExpressionTool
    execution_hooks: TransformationExecutionHooksHint
    scheduling: SchedulingHint

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (CommandLineTool, Workflow, ExpressionTool)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


# -----------------------------------------------------------------------------
# Production models
# -----------------------------------------------------------------------------


class ProductionSubmissionModel(BaseModel):
    """Production definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: Workflow
    # Key: step name, Value: description & execution_hooks of a transformation
    steps_execution_hooks: dict[str, TransformationExecutionHooksHint]
    # Key: step name, Value: scheduling configuration for a transformation
    steps_scheduling: dict[str, SchedulingHint] = {}

    @model_validator(mode="before")
    def validate_production(cls, values):
        task = values.get("task")

        # Metadata
        steps_execution_hooks = values.get("steps_execution_hooks")

        if task and steps_execution_hooks:
            # Extract the available steps in the task
            task_steps = set([step.id.split("#")[-1] for step in task.steps])
            metadata_keys = set(steps_execution_hooks.keys())

            # Check if all metadata keys exist in the task's workflow steps
            missing_steps = metadata_keys - task_steps
            if missing_steps:
                raise ValueError(
                    f"The following steps are missing from the task workflow: {missing_steps}"
                )

        # ResourceRequirement
        if any(req.class_ == "ResourceRequirement" for req in task.requirements):
            raise ValueError(
                "Global ResourceRequirement is not allowed in productions."
            )

        return values

    @field_serializer("task")
    def serialize_task(self, value):
        if isinstance(value, (ExpressionTool, CommandLineTool, Workflow)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


# -----------------------------------------------------------------------------
# Module helpers
# -----------------------------------------------------------------------------
def extract_dirac_hints(cwl: Any) -> tuple[ExecutionHooksHint, SchedulingHint]:
    """Thin wrapper that returns (ExecutionHooksHint, SchedulingHint).

    Prefer the class-factory APIs `ExecutionHooksHint.from_cwl` and
    `SchedulingHint.from_cwl` for new code. This helper remains for
    convenience.
    """
    return ExecutionHooksHint.from_cwl(cwl), SchedulingHint.from_cwl(cwl)


def validate_resource_requirements(task):
    """
    Validate ResourceRequirements of a task (CommandLineTool, Workflow, WorkflowStep, WorkflowStep.run).

    :param task: The task to validate
    """
    cwl_req = get_resource_requirement(task)

    # Validate Workflow/CLT requirements.
    if cwl_req:
        validate_resource_requirement(cwl_req)

    # Validate WorkflowStep requirements.
    if not isinstance(task, CommandLineTool) and task.steps:
        for step in task.steps:
            step_req = get_resource_requirement(step)
            if step_req:
                validate_resource_requirement(step_req, cwl_req=cwl_req)

            # Validate run requirements for each step if they exist.
            if step.run:
                if isinstance(step.run, Workflow):
                    # Validate nested Workflow requirements, if any.
                    validate_resource_requirements(task=step.run)

                step_run_req = get_resource_requirement(step.run)
                if step_run_req:
                    validate_resource_requirement(step_run_req, cwl_req=cwl_req)


def validate_resource_requirement(requirement, cwl_req=None):
    """
    Validate a ResourceRequirement.
    Verify:
     - that resourceMin is not higher than resourceMax (CommandLineTool, Workflow, WorkflowStep, WorkflowStep.run)
     --> #TODO this should be done by cwl-utils/cwltool later
     - that resourceMin (WorkflowStep, WorkflowStep.run) is not higher than global (Workflow) resourceMax.

    :param requirement: The current ResourceRequirement to validate.
    :param cwl_req: The global Workflow/CLT requirement, if any.
    :raises ValueError: If the requirement is invalid.
    """

    def check_resource(
        current_resource, req_min_value, req_max_value, global_max_value=None
    ):
        if req_min_value and req_max_value and req_min_value > req_max_value:
            raise ValueError(
                f"{current_resource}Min is higher than {current_resource}Max"
            )
        if global_max_value and req_min_value and req_min_value > global_max_value:
            raise ValueError(
                f"{current_resource}Min is higher than global {current_resource}Max"
            )

    for resource, min_value, max_value in [
        ("ram", requirement.ramMin, requirement.ramMax),
        ("cores", requirement.coresMin, requirement.coresMax),
        ("tmpdir", requirement.tmpdirMin, requirement.tmpdirMax),
        ("outdir", requirement.outdirMin, requirement.outdirMax),
    ]:
        check_resource(
            resource,
            min_value,
            max_value,
            cwl_req and getattr(cwl_req, f"{resource}Max"),
        )


def get_resource_requirement(
    cwl_object: Workflow | CommandLineTool | WorkflowStep,
) -> ResourceRequirement | None:
    """
    Extract the resource requirement from the current cwl_object

    :param cwl_object: The cwl_object to extract the requirement from.
    :return: The resource requirement object, or None if not found.
    """
    requirements = getattr(cwl_object, "requirements", []) or []
    for requirement in requirements:
        if requirement.class_ == "ResourceRequirement":
            return requirement
    return None
