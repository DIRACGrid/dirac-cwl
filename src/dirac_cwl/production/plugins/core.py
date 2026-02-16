"""Core input dataset plugins.

This module provides the built-in, experiment-agnostic input dataset plugins.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

from ..core import InputDatasetPluginBase


class NoOpInputDatasetPlugin(InputDatasetPluginBase):
    """No-operation input dataset plugin.

    This plugin does nothing and returns None for both inputs and catalog.
    Use this as a fallback when no input dataset generation is needed.
    """

    vo: ClassVar[str] = "generic"
    version: ClassVar[str] = "1.0.0"
    description: ClassVar[str] = "No-operation plugin that does not generate inputs"

    def generate_inputs(
        self,
        workflow_path: Path,
        config: dict[str, Any],
        output_dir: Path,
        n_lfns: int | None = None,
        pick_smallest: bool = False,
    ) -> tuple[Path | None, Path | None]:
        """Return None for both inputs and catalog (no-op).

        :return: (None, None) - no files generated.
        """
        return None, None
