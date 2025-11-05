"""
Enhanced submission models for DIRAC CWL integration.

This module provides improved submission models with proper separation of concerns,
modern Python typing, and comprehensive numpydoc documentation.
"""

from __future__ import annotations

from typing import Any

from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
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
        for req in task.requirements:
            if req.class_ == "ResourceRequirement":
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
