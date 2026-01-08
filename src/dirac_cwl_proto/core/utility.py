"""Utility functions for file catalog operations."""

from pathlib import Path
from typing import Any

from cwl_utils.parser.cwl_v1_2 import (
    File,
)


def get_lfns(input_data: dict[str, Any]) -> dict[str, list[Path]]:
    """Get the list of LFNs in the inputs from the parameters.

    :param input_data: The parameters of the job.
    :return: The list of LFN paths.
    """
    # Get the files from the input data
    files: dict[str, list[Path]] = {}
    for input_name, input_value in input_data.items():
        val = []
        if isinstance(input_value, list):
            for item in input_value:
                if isinstance(item, File):
                    if not item.location and not item.path:
                        raise NotImplementedError("File location is not defined.")

                    if not item.location:
                        continue
                    # Skip files from the File Catalog
                    if item.location.startswith("lfn:"):
                        val.append(Path(item.location))
            files[input_name] = val
        elif isinstance(input_value, File):
            if not input_value.location:
                raise NotImplementedError("File location is not defined.")
            if input_value.location.startswith("lfn:"):
                val.append(Path(input_value.location))
            files[input_name] = val
    return files
