"""Enhanced plugin registry for metadata models.

This module provides a sophisticated plugin discovery and registration system
for DIRAC metadata models, supporting virtual organization-specific extensions and
automatic discovery.
"""

from __future__ import annotations

import logging
from importlib.metadata import entry_points
from typing import Any, Dict, List, Optional, Type

from .core import ExecutionHooksBasePlugin, ExecutionHooksHint

logger = logging.getLogger(__name__)


class ExecutionHooksPluginRegistry:
    """
    Registry for execution hooks plugins.

    This class manages the registration and retrieval of execution hooks plugins
    for different steps in CWL workflows. Plugins are registered using
    entry points and can be retrieved by name.
    """

    def __init__(self) -> None:
        """Initialize the execution hooks registry."""
        self._plugins: Dict[str, Type[ExecutionHooksBasePlugin]] = {}
        self._vo_plugins: Dict[str, Dict[str, Type[ExecutionHooksBasePlugin]]] = {}
        self._plugin_info: Dict[str, Dict[str, Any]] = {}

    def register_plugin(self, plugin_class: Type[ExecutionHooksBasePlugin], override: bool = False) -> None:
        """Register a metadata plugin.

        :param plugin_class: The metadata model class to register.
        :type plugin_class: Type[ExecutionHooksBasePlugin]
        :param override: Whether to override existing registrations, by default False.
        :type override: bool
        :raises ValueError: If plugin is already registered and override=False.
        """
        if not issubclass(plugin_class, ExecutionHooksBasePlugin):
            raise ValueError(f"Plugin {plugin_class} must inherit from ExecutionHooksBasePlugin")

        plugin_key = plugin_class.name()
        vo = plugin_class.vo

        # Check for conflicts
        if plugin_key in self._plugins and not override:
            existing = self._plugins[plugin_key]
            raise ValueError(
                f"Plugin '{plugin_key}' already registered by {existing.__module__}.{existing.__name__}. "
                f"Use override=True to replace."
            )

        # Register globally
        self._plugins[plugin_key] = plugin_class
        self._plugin_info[plugin_key] = plugin_class.get_schema_info()

        # Register by VO if specified
        if vo:
            if vo not in self._vo_plugins:
                self._vo_plugins[vo] = {}
            self._vo_plugins[vo][plugin_key] = plugin_class

        logger.info(
            f"Registered metadata plugin '{plugin_key}' "
            f"from {plugin_class.__module__}.{plugin_class.__name__}"
            f"{f' (VO: {vo})' if vo else ''}"
        )

    def get_plugin(self, plugin_key: str, vo: Optional[str] = None) -> Optional[Type[ExecutionHooksBasePlugin]]:
        """Get a registered plugin.

        :param plugin_key: The plugin identifier.
        :type plugin_key: str
        :param vo: Virtual Organization namespace to search first, by default None.
        :type vo: Optional[str]
        :return: The plugin class or None if not found.
        :rtype: Optional[Type[ExecutionHooksBasePlugin]]
        """
        # Try VO-specific first if specified
        if vo and vo in self._vo_plugins:
            if plugin_key in self._vo_plugins[vo]:
                return self._vo_plugins[vo][plugin_key]

        # Fall back to global registry
        return self._plugins.get(plugin_key)

    def instantiate_plugin(self, descriptor: ExecutionHooksHint, **kwargs: Any) -> ExecutionHooksBasePlugin:
        """Instantiate a metadata plugin from a descriptor.

        :param descriptor: The data manager containing configuration.
        :type descriptor: ExecutionHooksHint
        :param kwargs: Additional parameters to pass to the plugin constructor.
        :type kwargs: Any
        :return: Instantiated metadata model.
        :rtype: ExecutionHooksBasePlugin
        :raises KeyError: If the requested plugin is not registered.
        :raises ValueError: If plugin instantiation fails.
        """
        plugin_class = self.get_plugin(descriptor.hook_plugin)

        if plugin_class is None:
            available = self.list_plugins()
            raise KeyError(f"Unknown execution hooks plugin: '{descriptor.hook_plugin}'" f"Available: {available}")

        # Extract plugin parameters from descriptor
        plugin_params = descriptor.model_dump(
            exclude={
                "hook_plugin",
            }
        )
        plugin_params.update(kwargs)

        try:
            return plugin_class(**plugin_params)
        except Exception as e:
            raise ValueError(f"Failed to instantiate plugin '{descriptor.hook_plugin}': {e}") from e

    def list_plugins(self, vo: Optional[str] = None) -> List[str]:
        """List available plugins.

        :param vo: Filter by Virtual Organization, by default None.
        :type vo: Optional[str]
        :return: List of available plugin keys.
        :rtype: List[str]
        """
        if vo and vo in self._vo_plugins:
            return list(self._vo_plugins[vo].keys())
        return list(self._plugins.keys())

    def list_virtual_organizations(self) -> List[str]:
        """List available Virtual Organizations."""
        return list(self._vo_plugins.keys())

    def get_plugin_info(self, plugin_key: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a plugin."""
        return self._plugin_info.get(plugin_key)

    def discover_plugins(self) -> int:
        """Discover and register plugins from the entry points defined in the pyproject.toml.

        :return: Number of plugins discovered and registered.
        :rtype: int
        """
        entrypoints = entry_points(group="dirac_cwl_proto.execution_hooks")
        discovered = 0
        for hook_name in entrypoints.names:
            try:
                hook = entrypoints[hook_name].load()
                if issubclass(hook, ExecutionHooksBasePlugin):
                    self.register_plugin(hook)
                    discovered += 1
                else:
                    logger.warning(
                        "Tried to discover execution hook with name '%s' that does not inherit %s",
                        hook_name,
                        ExecutionHooksBasePlugin.__name__,
                    )
            except Exception as e:
                logger.error(f"Failed to import plugin {hook_name}: {e}")

        return discovered

    def validate_descriptor(self, descriptor: ExecutionHooksHint) -> List[str]:
        """Validate a data manager against registered plugins.

        :param descriptor: The data manager to validate.
        :type descriptor: ExecutionHooksHint
        :return: List of validation errors (empty if valid).
        :rtype: List[str]
        """
        errors = []

        plugin_class = self.get_plugin(descriptor.hook_plugin)

        if plugin_class is None:
            available = self.list_plugins()
            errors.append(f"Unknown metadata plugin: '{descriptor.hook_plugin}'. " f"Available: {available}")
            return errors

        # Validate descriptor against plugin schema
        try:
            plugin_params = descriptor.model_dump(exclude={"hook_plugin"})
            plugin_class.model_validate(plugin_params)
        except Exception as e:
            errors.append(f"Plugin validation failed: {e}")

        return errors


# Global registry instance
_registry = ExecutionHooksPluginRegistry()


# Public API
def get_registry() -> ExecutionHooksPluginRegistry:
    """Get the global execution hooks plugin registry."""
    return _registry


def discover_plugins() -> int:
    """Discover and register plugins from packages."""
    return _registry.discover_plugins()
