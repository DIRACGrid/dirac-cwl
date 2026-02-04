"""Plugin package initialization.

This module ensures that core plugins are automatically registered
when the metadata system is imported.
"""

from .core import (
    QueryBasedPlugin,
)

# Plugins will be auto-registered through the metaclass or discovery system

__all__ = [
    # Core plugins
    "QueryBasedPlugin",
]
