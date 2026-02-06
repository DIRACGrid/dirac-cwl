"""Core classes for the Production plugin system.

This module provides the foundational classes for the input dataset plugin system,
allowing experiment-specific input data sources (e.g., LHCb Bookkeeping) to be
integrated via plugins.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field


class InputDatasetPluginBase(ABC):
    """Abstract base class for input dataset plugins.

    Input dataset plugins are responsible for querying external data catalogs
    (e.g., LHCb Bookkeeping, CTAO Rucio) and generating the input files needed
    for local CWL execution.

    Subclasses must implement the `generate_inputs` method.
    """

    vo: ClassVar[str] = "generic"
    version: ClassVar[str] = "1.0.0"
    description: ClassVar[str] = "Base input dataset plugin"

    @classmethod
    def name(cls) -> str:
        """Return the plugin identifier (class name by default)."""
        return cls.__name__

    @abstractmethod
    def generate_inputs(
        self,
        workflow_path: Path,
        config: dict[str, Any],
        output_dir: Path,
        n_lfns: int | None = None,
        pick_smallest: bool = False,
    ) -> tuple[Path | None, Path | None]:
        """Generate inputs.yml and catalog.json files for CWL execution.

        :param workflow_path: Path to the CWL workflow file.
        :param config: Plugin-specific configuration from the hint.
        :param output_dir: Directory to write output files.
        :param n_lfns: Optional limit on number of LFNs to include.
        :param pick_smallest: If True, select smallest files first.
        :return: Tuple of (inputs_path, catalog_path), either may be None.
        """

    def format_hint_display(self, config: dict[str, Any]) -> list[tuple[str, str]]:
        """Format configuration for CLI display.

        Override this method to provide experiment-specific display formatting.

        :param config: The input_dataset_config from the hint.
        :return: List of (key, value) tuples for display.
        """
        return []

    @classmethod
    def get_schema_info(cls) -> dict[str, Any]:
        """Get schema information for this plugin."""
        return {
            "plugin_name": cls.name(),
            "vo": cls.vo,
            "version": cls.version,
            "description": cls.description,
        }


class ProductionHint(BaseModel):
    """Model for the dirac:Production CWL hint.

    This hint configures how input data is resolved for a production workflow.
    """

    model_config = ConfigDict(
        extra="allow",
        validate_assignment=True,
    )

    input_dataset_plugin: str | None = Field(
        default=None,
        description="Name of the input dataset plugin to use",
    )

    input_dataset_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Plugin-specific configuration for input dataset generation",
    )

    @classmethod
    def from_cwl(cls, cwl_content: dict[str, Any]) -> ProductionHint | None:
        """Extract dirac:Production hint from CWL content.

        :param cwl_content: Parsed CWL document as a dictionary.
        :return: ProductionHint if found, None otherwise.
        """
        hints = cwl_content.get("hints", [])
        if not hints:
            return None

        for hint in hints:
            if hint.get("class") == "dirac:Production":
                hint_data = {k: v for k, v in hint.items() if k != "class"}
                return cls(**hint_data)

        return None

    def to_runtime(self) -> InputDatasetPluginBase:
        """Instantiate the configured input dataset plugin.

        :return: Instance of the configured plugin.
        :raises KeyError: If the plugin is not registered.
        """
        from .registry import get_registry

        return get_registry().instantiate(self)
