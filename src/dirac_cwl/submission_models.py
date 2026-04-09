"""
Enhanced submission models for DIRAC CWL integration.

This module provides improved submission models with proper separation of concerns,
modern Python typing, and comprehensive numpydoc documentation.
"""

from __future__ import annotations

from typing import Any, Optional

from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
    ResourceRequirement,
    Workflow,
)
from pydantic import BaseModel, ConfigDict, field_serializer, model_validator

from dirac_cwl.execution_hooks import (
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
        """Serialize CWL object to dictionary.

        :param value: CWL object to serialize.
        :return: Serialized CWL dictionary.
        """
        return save(value)


class BaseJobModel(BaseModel):
    """Base class for Job definition."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: CommandLineTool | Workflow | ExpressionTool

    @model_validator(mode="before")
    def validate_job(cls, values):
        """Validate job workflow.

        :param values: Model values dictionary.
        :return: Validated values dictionary.
        """
        task = values.get("task")

        # ResourceRequirement validation
        validate_resource_requirements(task)

        # Hints validation
        ExecutionHooksHint.from_cwl(task), SchedulingHint.from_cwl(task)

        return values

    @field_serializer("task")
    def serialize_task(self, value):
        """Serialize CWL task object to dictionary.

        :param value: CWL task object to serialize.
        :return: Serialized task dictionary.
        :raises TypeError: If value is not a valid CWL task type.
        """
        if isinstance(value, (CommandLineTool, Workflow, ExpressionTool)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")


class JobSubmissionModel(BaseJobModel):
    """Job definition sent to the router."""

    inputs: list[JobInputModel] | None = None


class JobModel(BaseJobModel):
    """Job definition sent to the job wrapper."""

    input: Optional[JobInputModel] = None


# -----------------------------------------------------------------------------
# Transformation models
# -----------------------------------------------------------------------------
class TransformationSubmissionModel(BaseModel):
    """Transformation definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: CommandLineTool | Workflow | ExpressionTool

    @field_serializer("task")
    def serialize_task(self, value):
        """Serialize CWL task object to dictionary.

        :param value: CWL task object to serialize.
        :return: Serialized task dictionary.
        :raises TypeError: If value is not a valid CWL task type.
        """
        if isinstance(value, (CommandLineTool, Workflow, ExpressionTool)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")

    @model_validator(mode="before")
    def validate_transformation(cls, values):
        """Validate transformation workflow.

        :param values: Model values dictionary.
        :return: Validated values dictionary.
        """
        task = values.get("task")

        # ResourceRequirement validation
        validate_resource_requirements(task)

        # Hints validation
        TransformationExecutionHooksHint.from_cwl(task), SchedulingHint.from_cwl(task)

        return values


# -----------------------------------------------------------------------------
# Production models
# -----------------------------------------------------------------------------


class ProductionSubmissionModel(BaseModel):
    """Production definition sent to the router."""

    # Allow arbitrary types to be passed to the model
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: Workflow

    @field_serializer("task")
    def serialize_task(self, value):
        """Serialize CWL workflow object to dictionary.

        :param value: CWL workflow object to serialize.
        :return: Serialized workflow dictionary.
        :raises TypeError: If value is not a valid CWL workflow type.
        """
        if isinstance(value, (ExpressionTool, CommandLineTool, Workflow)):
            return save(value)
        else:
            raise TypeError(f"Cannot serialize type {type(value)}")

    @model_validator(mode="before")
    def validate_production(cls, values):
        """Validate production workflow."""
        task = values.get("task")

        # ResourceRequirement validation
        validate_resource_requirements(task)

        return values


# -----------------------------------------------------------------------------
# ResourceRequirement validations
# -----------------------------------------------------------------------------
# Temporary code, waiting on cwltool PR: https://github.com/common-workflow-language/cwltool/pull/2179.


def validate_resource_requirements(task: CommandLineTool | Workflow | ExpressionTool):
    """Validate ResourceRequirements of a task recursively.

    :param task: The task to validate.
    :raises ValueError: If any ResourceRequirement has min > max.
    """
    # Validate task-level requirements
    for req in getattr(task, "requirements", None) or []:
        if isinstance(req, ResourceRequirement):
            _validate_min_max(req)

    # Recurse into workflow steps
    if isinstance(task, Workflow):
        for step in task.steps or []:
            for req in getattr(step, "requirements", None) or []:
                if isinstance(req, ResourceRequirement):
                    _validate_min_max(req)
            if step.run:
                validate_resource_requirements(step.run)


def _validate_min_max(req: ResourceRequirement):
    """Check that min does not exceed max for any resource.

    :param req: The ResourceRequirement to validate.
    :raises ValueError: If min > max for any resource.
    """
    for name, lo, hi in [
        ("cores", req.coresMin, req.coresMax),
        ("ram", req.ramMin, req.ramMax),
        ("tmpdir", req.tmpdirMin, req.tmpdirMax),
        ("outdir", req.outdirMin, req.outdirMax),
    ]:
        if lo and hi and lo > hi:
            raise ValueError(f"{name}Min ({lo}) exceeds {name}Max ({hi})")
