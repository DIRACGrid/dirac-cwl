"""Post-processing command that stores output files to grid storage."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Sequence

from DIRAC.DataManagementSystem.Client.DataManager import DataManager  # type: ignore[import-untyped]
from DIRACCommon.Core.Utilities.ReturnValues import returnSingleResult  # type: ignore[import-untyped]

from dirac_cwl.commands import PostProcessCommand
from dirac_cwl.mocks.data_manager import MockDataManager

logger = logging.getLogger(__name__)


class StoreOutputCommand(PostProcessCommand):
    """Store output files to grid storage elements via DataManager.

    Replaces the output storage logic previously in ExecutionHooksBasePlugin.
    """

    def __init__(
        self,
        output_paths: dict[str, str],
        output_se: list[str],
    ) -> None:
        self._output_paths = output_paths
        self._output_se = output_se
        if os.getenv("DIRAC_PROTO_LOCAL") == "1":
            self._datamanager: DataManager = MockDataManager()
        else:
            self._datamanager = DataManager()

    async def execute(self, job_path: Path, **kwargs: Any) -> None:
        """Store output files to grid storage.

        :param job_path: Path to the job working directory.
        :param kwargs: Must include 'outputs' dict mapping output names to file paths.
        """
        outputs: dict[str, str | Path | Sequence[str | Path]] = kwargs.get("outputs", {})

        for output_name, src_path in outputs.items():
            if not src_path:
                raise RuntimeError(
                    f"src_path parameter required for filesystem storage of {output_name}"
                )

            lfn = self._output_paths.get(output_name, None)

            if lfn:
                logger.info("Storing output %s, with source %s", output_name, src_path)
                if isinstance(src_path, (str, Path)):
                    src_path = [src_path]
                for src in src_path:
                    file_lfn = Path(lfn) / Path(src).name
                    res = None
                    for se in self._output_se:
                        res = returnSingleResult(
                            self._datamanager.putAndRegister(str(file_lfn), src, se)
                        )
                        if res["OK"]:
                            logger.info(
                                "Successfully saved file %s with LFN %s", src, file_lfn
                            )
                            break
                    if res and not res["OK"]:
                        raise RuntimeError(
                            f"Could not save file {src} with LFN {str(lfn)} : {res['Message']}"
                        )
