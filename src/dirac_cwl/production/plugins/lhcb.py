"""LHCb-specific input dataset plugin.

This module provides the LHCb Bookkeeping integration for input dataset generation.
"""

from __future__ import annotations

import importlib.util
import logging
import subprocess
from pathlib import Path
from typing import Any, ClassVar

from ..core import InputDatasetPluginBase

logger = logging.getLogger(__name__)


class LHCbBookkeepingPlugin(InputDatasetPluginBase):
    """LHCb Bookkeeping input dataset plugin.

    This plugin queries the LHCb Bookkeeping system via the `generate_replica_catalog`
    module from LbAPLocal. It wraps the existing functionality and runs it as a
    subprocess using `lb-dirac python`.
    """

    vo: ClassVar[str] = "lhcb"
    version: ClassVar[str] = "1.0.0"
    description: ClassVar[str] = "LHCb Bookkeeping input dataset plugin"

    def generate_inputs(
        self,
        workflow_path: Path,
        config: dict[str, Any],
        output_dir: Path,
        n_lfns: int | None = None,
        pick_smallest: bool = False,
    ) -> tuple[Path | None, Path | None]:
        """Generate inputs and catalog from LHCb Bookkeeping.

        This method finds the generate_replica_catalog.py script from LbAPLocal
        and runs it via `lb-dirac python` to query the Bookkeeping.

        :param workflow_path: Path to the CWL workflow file.
        :param config: Plugin configuration from the hint (event_type, conditions_dict, etc.).
        :param output_dir: Directory to write output files.
        :param n_lfns: Optional limit on number of LFNs to include.
        :param pick_smallest: If True, select smallest files first.
        :return: Tuple of (inputs_path, catalog_path).
        :raises RuntimeError: If the generator script fails.
        """
        # Determine output paths
        inputs_path = output_dir / f"{workflow_path.stem}-inputs.yml"
        map_path = output_dir / f"{workflow_path.stem}-replica-map.json"

        # Find the generate_replica_map.py script
        spec = importlib.util.find_spec("LbAPLocal.cwl.generate_replica_map")
        if spec is None or spec.origin is None:
            raise RuntimeError(
                "Could not find LbAPLocal.cwl.generate_replica_map module. " "Ensure LbAPLocal is installed."
            )

        generate_replica_map_path = spec.origin

        # Build command to run generate_replica_map.py via lb-dirac python
        cmd = [
            "lb-dirac",
            "python",
            generate_replica_map_path,
            str(workflow_path),
            "--output-yaml",
            str(inputs_path),
            "--output-map",
            str(map_path),
        ]

        # Add optional arguments
        if n_lfns is not None:
            cmd.extend(["--n-lfns", str(n_lfns)])

        if pick_smallest:
            cmd.append("--pick-smallest-lfn")

        logger.info("Running LHCb replica map generator: %s", " ".join(cmd))

        # Run the subprocess
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        # Check if the command succeeded
        if result.returncode != 0:
            error_msg = (
                f"generate_replica_map.py failed with exit code {result.returncode}\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}"
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        if result.stdout:
            logger.info("Generator output: %s", result.stdout)

        return inputs_path, map_path

    def format_hint_display(self, config: dict[str, Any]) -> list[tuple[str, str]]:
        """Format LHCb-specific configuration for display.

        :param config: The input_dataset_config from the hint.
        :return: List of (key, value) tuples for display.
        """
        display_items = []

        if "event_type" in config:
            display_items.append(("EventType", str(config["event_type"])))

        if "conditions_description" in config:
            display_items.append(("Conditions", config["conditions_description"]))

        conditions_dict = config.get("conditions_dict", {})
        if "configName" in conditions_dict:
            display_items.append(("Config", conditions_dict["configName"]))

        if "inFileType" in conditions_dict:
            display_items.append(("FileType", conditions_dict["inFileType"]))

        if "inProPass" in conditions_dict:
            display_items.append(("ProcessingPass", conditions_dict["inProPass"]))

        return display_items
