"""Core metadata framework for DIRAC CWL integration.

This module provides the foundational classes and interfaces for the extensible
metadata plugin system in DIRAC/DIRACX.
"""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Self,
    Sequence,
    TypeVar,
    Union,
)

from DIRAC.DataManagementSystem.Client.DataManager import (  # type: ignore[import-untyped]
    DataManager,
)
from DIRACCommon.Core.Utilities.ReturnValues import (  # type: ignore[import-untyped]
    returnSingleResult,
)
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from dirac_cwl.commands import PostProcessCommand, PreProcessCommand
from dirac_cwl.data_management_mocks.data_manager import MockDataManager

logger = logging.getLogger(__name__)

# TypeVar for generic class methods
T = TypeVar("T", bound="SchedulingHint")


class ExecutionHooksBasePlugin(BaseModel):
    """Base class for all runtime plugin models with execution hooks.

    This class uses composition instead of inheritance for data catalog operations,
    providing better separation of concerns and flexibility.
    """

    model_config = ConfigDict(
        extra="ignore",
        validate_assignment=True,
        arbitrary_types_allowed=True,
        json_schema_extra={
            "title": "DIRAC Metadata Model",
            "description": "Base metadata model for DIRAC jobs",
        },
    )

    # Class-level metadata for plugin discovery
    vo: ClassVar[Optional[str]] = None
    version: ClassVar[str] = "1.0.0"
    description: ClassVar[str] = "Base metadata model"

    output_paths: Dict[str, Any] = {}
    output_sandbox: list[str] = []
    output_se: list[str] = []

    _datamanager: DataManager = PrivateAttr(default_factory=DataManager)

    def __init__(self, **kwargs):
        """Initialize the execution hooks base plugin.

        :param kwargs: Additional keyword arguments passed to the parent class.
        """
        super().__init__(**kwargs)
        if os.getenv("DIRAC_PROTO_LOCAL") == "1":
            self._datamanager = MockDataManager()

    _preprocess_commands: List[type[PreProcessCommand]] = PrivateAttr(default=[])
    _postprocess_commands: List[type[PostProcessCommand]] = PrivateAttr(default=[])

    @property
    def preprocess_commands(self) -> List[type[PreProcessCommand]]:
        """Get the list of pre-processing commands."""
        return self._preprocess_commands

    @preprocess_commands.setter
    def preprocess_commands(self, value: List[type[PreProcessCommand]]) -> None:
        """Set the list of pre-processing commands."""
        self._preprocess_commands = value

    @property
    def postprocess_commands(self) -> List[type[PostProcessCommand]]:
        """Get the list of post-processing commands."""
        return self._postprocess_commands

    @postprocess_commands.setter
    def postprocess_commands(self, value: List[type[PostProcessCommand]]) -> None:
        """Set the list of post-processing commands."""
        self._postprocess_commands = value

    @classmethod
    def name(cls) -> str:
        """Auto-derive hook plugin identifier from class name."""
        return cls.__name__

    async def store_output(
        self,
        outputs: dict[str, str | Path | Sequence[str | Path]],
        **kwargs: Any,
    ) -> None:
        """Store an output file or set of files via the appropriate storage interface.

        :param dict[str, str | Path | Sequence[str | Path]] outputs:
            Dictionary containing the path or list of paths to the source file(s) to be stored
            for each cwl output.
        :param Any **kwargs:
            Additional keyword arguments for extensibility.
        """
        if os.getenv("DIRAC_PROTO_LOCAL") == "1":
            from dirac_cwl.data_management_mocks.status import set_job_status  # type: ignore[no-redef]
        else:
            from diracx.api.jobs import set_job_status  # type: ignore[no-redef]
        for output_name, src_path in outputs.items():
            if not src_path:
                raise RuntimeError(f"src_path parameter required for filesystem storage of {output_name}")

            lfn = self.output_paths.get(output_name, None)

            if lfn:
                logger.info("Storing output %s, with source %s", output_name, src_path)
                jobID = os.environ["JOBID"] if "JOBID" in os.environ else "0"
                await set_job_status(jobID, minor_status="Uploading Output Data", source="JobWrapper")
                if isinstance(src_path, str) or isinstance(src_path, Path):
                    src_path = [src_path]
                for src in src_path:
                    file_lfn = Path(lfn) / Path(src).name
                    res = None
                    for se in self.output_se:
                        res = returnSingleResult(self._datamanager.putAndRegister(str(file_lfn), src, se))
                        if res["OK"]:
                            logger.info("Successfully saved file %s with LFN %s", src, file_lfn)
                            break
                    if res and not res["OK"]:
                        await set_job_status(
                            jobID, status="Failed", minor_status="Uploading Output Data", source="JobWrapper"
                        )
                        raise RuntimeError(f"Could not save file {src} with LFN {str(lfn)} : {res['Message']}")
                await set_job_status(
                    jobID, status="Completing", minor_status="Output Data Uploaded", source="JobWrapper"
                )

    def get_input_query(self, input_name: str, **kwargs: Any) -> Union[Path, List[Path], None]:
        """Generate LFN-based input query path.

        Accepts and ignores extra kwargs for interface compatibility.
        """
        # Build LFN: /query_root/vo/campaign/site/data_type/input_name
        pass

    @classmethod
    def get_schema_info(cls) -> Dict[str, Any]:
        """Get schema information for this metadata model."""
        return {
            "hook_plugin": cls.name(),
            "vo": cls.vo,
            "version": cls.version,
            "description": cls.description,
            "schema": cls.model_json_schema(),
        }


class Hint(ABC):
    """Base class for all DIRAC hints and requirements models."""

    @classmethod
    @abstractmethod
    def from_cwl(cls, cwl_object: Any) -> "Hint":
        """Extract hint information from a CWL object."""
        pass


class SchedulingHint(BaseModel, Hint):
    """Descriptor for job execution configuration."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    platform: Optional[str] = Field(default=None, description="Target platform (e.g., 'DIRAC', 'DIRACX')")

    priority: int = Field(default=10, description="Job priority (higher values = higher priority)")

    sites: Optional[List[str]] = Field(default=None, description="Candidate execution sites")

    @classmethod
    def from_cwl(cls: type[T], cwl_object: Any) -> T:
        """Extract task descriptor from CWL hints."""
        descriptor = cls()

        hints = getattr(cwl_object, "hints", []) or []
        for hint in hints:
            if hint.get("class") == "dirac:Scheduling":
                hint_data = {k: v for k, v in hint.items() if k != "class"}
                descriptor = descriptor.model_copy(update=hint_data)

        return descriptor


class ExecutionHooksHint(BaseModel, Hint):
    """Descriptor for data management configuration in CWL hints.

    This class represents the serializable data management configuration that
    can be embedded in CWL hints and later instantiated into concrete
    metadata models.

    Enhanced with submission functionality for DIRAC CWL integration.
    """

    model_config = ConfigDict(
        extra="allow",  # Allow vo-specific fields
        validate_assignment=True,
        json_schema_extra={
            "title": "DIRAC Data Manager",
            "description": "Data management configuration for DIRAC jobs",
        },
    )

    hook_plugin: str = Field(
        default="QueryBasedPlugin",
        description="Registry key for the metadata implementation class",
    )

    # Enhanced fields for submission functionality
    configuration: Dict[str, Any] = Field(
        default_factory=dict, description="Additional parameters for metadata plugins"
    )

    output_paths: Dict[str, Any] = Field(default_factory=dict, description="LFNs for outputs on the Data Catalog")

    output_sandbox: list[str] = Field(
        default_factory=list,
        description="List of the outputs stored in the output sandbox",
    )

    output_se: list[str] = Field(
        default_factory=lambda: ["SE-USER"],
        description="List of Storage Elements that can be used to store the outputs",
    )

    def model_copy(
        self,
        update: Optional[Mapping[str, Any]] = None,
        *,
        deep: bool = False,
    ) -> Self:
        """Enhanced model copy with intelligent merging of dict fields (including configuration)."""
        if update is None:
            update = {}
        else:
            update = dict(update)

        merged_update = {}
        for key, value in update.items():
            if hasattr(self, key) and isinstance(getattr(self, key), dict) and isinstance(value, dict):
                existing_value = getattr(self, key).copy()
                existing_value.update(value)
                merged_update[key] = existing_value
            else:
                merged_update[key] = value

        return super().model_copy(update=merged_update, deep=deep)

    def to_runtime(self, submitted: Optional[Any] = None) -> "ExecutionHooksBasePlugin":
        """Build and instantiate the runtime metadata implementation.

        The returned object is an instance of :class:`ExecutionHooksBasePlugin` created
        by the metadata registry. The instantiation parameters are constructed
        by merging, in order:

        1. Input defaults declared on the CWL task (if ``submitted`` is provided).
        2. The first set of CWL parameter overrides (``submitted.parameters``),
           if present.
        3. The descriptor's ``configuration``.

        During merging, keys are normalized from dash-case to snake_case to
        align with typical Python argument names used by runtime implementations.

        :param submitted: Optional submission context used to resolve CWL input defaults
            and parameter overrides.
        :type submitted: JobSubmissionModel | None
        :return: Runtime plugin implementation instantiated from the registry.
        :rtype: ExecutionHooksBasePlugin
        """
        # Import here to avoid circular imports
        from .registry import get_registry

        # Quick helper to convert dash-case to snake_case without importing utils
        def _dash_to_snake(s: str) -> str:
            return s.replace("-", "_")

        if submitted is None:
            descriptor = ExecutionHooksHint(
                hook_plugin=self.hook_plugin,
                output_paths=self.output_paths,
                output_sandbox=self.output_sandbox,
                output_se=self.output_se,
                **self.configuration,
            )
            return get_registry().instantiate_plugin(descriptor)

        # Build inputs from task defaults and parameter overrides
        inputs: dict[str, Any] = {}
        for inp in submitted.task.inputs:
            input_name = inp.id.split("#")[-1].split("/")[-1]
            input_value = getattr(inp, "default", None)
            params_list = getattr(submitted, "parameters", None)
            if params_list and params_list[0]:
                input_value = params_list[0].cwl.get(input_name, input_value)
            inputs[input_name] = input_value

        # Merge with explicit configuration
        if self.configuration:
            inputs.update(self.configuration)

        params = {_dash_to_snake(key): value for key, value in inputs.items()}

        descriptor = ExecutionHooksHint(
            hook_plugin=self.hook_plugin,
            output_paths=self.output_paths,
            output_sandbox=self.output_sandbox,
            output_se=self.output_se,
            **params,
        )
        return get_registry().instantiate_plugin(descriptor)

    @classmethod
    def from_cwl(cls, cwl_object: Any) -> Self:
        """Extract metadata descriptor from CWL object using Hint interface."""
        descriptor = cls()
        hints = getattr(cwl_object, "hints", []) or []
        for hint in hints:
            if hint.get("class") == "dirac:ExecutionHooks":
                hint_data = {k: v for k, v in hint.items() if k != "class"}
                descriptor = descriptor.model_copy(update=hint_data)
        return descriptor


class TransformationExecutionHooksHint(ExecutionHooksHint):
    """Extended data manager for transformations."""

    group_size: Optional[Dict[str, int]] = Field(
        default=None, description="Input grouping configuration for transformation jobs"
    )
