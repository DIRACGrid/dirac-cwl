"""Input dataset plugins for the Production system.

This package contains the built-in input dataset plugins.
"""

from .core import NoOpInputDatasetPlugin
from .lhcb import LHCbBookkeepingPlugin

__all__ = [
    "NoOpInputDatasetPlugin",
    "LHCbBookkeepingPlugin",
]
