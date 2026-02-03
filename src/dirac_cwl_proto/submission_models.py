"""
Enhanced submission models for DIRAC CWL integration.

This module provides improved submission models with proper separation of concerns,
modern Python typing, and comprehensive numpydoc documentation.
"""

from __future__ import annotations

from typing import Any, Optional

from cwl_utils.parser import File, save
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
    def validate_hints(cls, values):
        """Validate execution hooks and scheduling hints in the task.

        :param values: Model values dictionary.
        :return: Validated values dictionary.
        """
        task = values.get("task")
        ExecutionHooksHint.from_cwl(task), SchedulingHint.from_cwl(task)
        return values


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
    input_data: Optional[list[str | File] | None] = None

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

    @field_serializer("input_data")
    def serialize_input_data(self, value):
        """Serialize an input data list to a list of strings.

        :param value: Input data list to serialize.
        :return: Serialized input data list.
        """
        if value:
            return [save(item) if isinstance(item, File) else item for item in value]
        return None

    @model_validator(mode="before")
    def validate_hints(cls, values):
        """Validate transformation execution hooks and scheduling hints in the task.

        :param values: Model values dictionary.
        :return: Validated values dictionary.
        """
        task = values.get("task")
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
