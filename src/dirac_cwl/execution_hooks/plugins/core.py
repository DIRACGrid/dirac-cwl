"""Core DIRAC metadata models.

This module contains the standard metadata models provided by DIRAC core.
These serve as examples and provide basic functionality for common use cases.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union

from pydantic import Field

from ..core import (
    ExecutionHooksBasePlugin,
)

logger = logging.getLogger(__name__)


class QueryBasedPlugin(ExecutionHooksBasePlugin):
    """Metadata plugin using LFN-based data catalog for structured data discovery.

    This plugin demonstrates filesystem-based data organization using
    Logical File Names (LFNs) with campaign, site, and data type parameters.
    """

    description: ClassVar[str] = "LFN-based metadata for structured data discovery"

    # LFN parameters
    query_root: str = Field(default="/grid/data", description="Base path for LFN structure")
    site: Optional[str] = Field(default=None, description="Site identifier for LFN path")
    campaign: Optional[str] = Field(default=None, description="Campaign name for LFN path")
    data_type: Optional[str] = Field(default=None, description="Data type classification")

    def get_input_query(self, input_name: str, **kwargs: Any) -> Union[Path, List[Path], None]:
        """Generate LFN-based input query path.

        Accepts and ignores extra kwargs for interface compatibility.
        """
        # Build LFN: /query_root/vo/campaign/site/data_type/input_name
        path_parts = []

        if self.vo:
            path_parts.append(self.vo)

        if self.campaign:
            path_parts.append(self.campaign)
        if self.site:
            path_parts.append(self.site)
        if self.data_type:
            path_parts.append(self.data_type)

        if len(path_parts) > 0:  # More than just VO
            return Path(self.query_root) / Path(*path_parts) / Path(input_name)

        return Path(self.query_root) / Path(input_name)
