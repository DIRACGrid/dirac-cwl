"""Enhanced metadata registry for DIRAC CWL integration.

This module provides a comprehensive plugin system for metadata models,
supporting virtual organization-specific extensions and automatic discovery.

The module maintains backward compatibility with the original API while
providing enhanced functionality through the new plugin system.
"""

from .core import (
    ExecutionHooksBasePlugin,
    ExecutionHooksHint,
    SchedulingHint,
    TransformationExecutionHooksHint,
)
from .registry import (
    ExecutionHooksPluginRegistry,
    discover_plugins,
    get_registry,
)

# Initialize the registry and discover core plugins
_registry = get_registry()

# Auto-discover plugins on import
try:
    discover_plugins()
except Exception:
    # Fail silently if plugin discovery fails during import
    pass

__all__ = [
    # Core metadata and plugins
    "ExecutionHooksHint",
    "TransformationExecutionHooksHint",
    "ExecutionHooksBasePlugin",
    "SchedulingHint",
    "ExecutionHooksPluginRegistry",
    "get_registry",
]
