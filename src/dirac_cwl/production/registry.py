"""Plugin registry for input dataset plugins.

This module provides the registry for discovering and managing input dataset
plugins via Python entry points.
"""

from __future__ import annotations

import logging
from importlib.metadata import entry_points
from typing import Any

from .core import InputDatasetPluginBase, ProductionHint

logger = logging.getLogger(__name__)


class InputDatasetPluginRegistry:
    """Registry for input dataset plugins.

    This class manages the registration and retrieval of input dataset plugins.
    Plugins are discovered via entry points and can be retrieved by name.
    """

    ENTRY_POINT_GROUP = "dirac_cwl.input_dataset_plugins"

    def __init__(self) -> None:
        """Initialize the input dataset plugin registry."""
        self._plugins: dict[str, type[InputDatasetPluginBase]] = {}
        self._vo_plugins: dict[str, dict[str, type[InputDatasetPluginBase]]] = {}
        self._plugin_info: dict[str, dict[str, Any]] = {}

    def register_plugin(self, plugin_class: type[InputDatasetPluginBase], override: bool = False) -> None:
        """Register an input dataset plugin.

        :param plugin_class: The plugin class to register.
        :param override: Whether to override existing registrations.
        :raises ValueError: If plugin is already registered and override=False.
        """
        if not issubclass(plugin_class, InputDatasetPluginBase):
            raise ValueError(f"Plugin {plugin_class} must inherit from InputDatasetPluginBase")

        plugin_key = plugin_class.name()
        vo = plugin_class.vo

        if plugin_key in self._plugins and not override:
            existing = self._plugins[plugin_key]
            raise ValueError(
                f"Plugin '{plugin_key}' already registered by "
                f"{existing.__module__}.{existing.__name__}. "
                f"Use override=True to replace."
            )

        self._plugins[plugin_key] = plugin_class
        self._plugin_info[plugin_key] = plugin_class.get_schema_info()

        if vo and vo != "generic":
            if vo not in self._vo_plugins:
                self._vo_plugins[vo] = {}
            self._vo_plugins[vo][plugin_key] = plugin_class

        vo_suffix = f" (VO: {vo})" if vo and vo != "generic" else ""
        logger.info(
            "Registered input dataset plugin '%s' from %s.%s%s",
            plugin_key,
            plugin_class.__module__,
            plugin_class.__name__,
            vo_suffix,
        )

    def get_plugin(self, plugin_key: str, vo: str | None = None) -> type[InputDatasetPluginBase] | None:
        """Get a registered plugin by name.

        :param plugin_key: The plugin identifier.
        :param vo: Virtual Organization namespace to search first.
        :return: The plugin class or None if not found.
        """
        if vo and vo in self._vo_plugins:
            if plugin_key in self._vo_plugins[vo]:
                return self._vo_plugins[vo][plugin_key]

        return self._plugins.get(plugin_key)

    def instantiate(self, hint: ProductionHint, **kwargs: Any) -> InputDatasetPluginBase:
        """Instantiate a plugin from a ProductionHint.

        :param hint: The production hint containing plugin configuration.
        :param kwargs: Additional parameters to pass to the plugin constructor.
        :return: Instantiated plugin.
        :raises KeyError: If the requested plugin is not registered.
        :raises ValueError: If plugin instantiation fails.
        """
        if hint.input_dataset_plugin is None:
            raise ValueError("No input_dataset_plugin specified in hint")

        plugin_class = self.get_plugin(hint.input_dataset_plugin)

        if plugin_class is None:
            available = self.list_plugins()
            raise KeyError(f"Unknown input dataset plugin: '{hint.input_dataset_plugin}'. " f"Available: {available}")

        try:
            return plugin_class(**kwargs)
        except Exception as e:
            raise ValueError(f"Failed to instantiate plugin '{hint.input_dataset_plugin}': {e}") from e

    def list_plugins(self, vo: str | None = None) -> list[str]:
        """List available plugins.

        :param vo: Filter by Virtual Organization.
        :return: List of available plugin names.
        """
        if vo and vo in self._vo_plugins:
            return list(self._vo_plugins[vo].keys())
        return list(self._plugins.keys())

    def list_virtual_organizations(self) -> list[str]:
        """List Virtual Organizations with registered plugins."""
        return list(self._vo_plugins.keys())

    def get_plugin_info(self, plugin_key: str) -> dict[str, Any] | None:
        """Get detailed information about a plugin."""
        return self._plugin_info.get(plugin_key)

    def discover_plugins(self) -> int:
        """Discover and register plugins from entry points.

        :return: Number of plugins discovered and registered.
        """
        eps = entry_points(group=self.ENTRY_POINT_GROUP)
        discovered = 0

        for ep in eps:
            try:
                plugin_class = ep.load()
                if issubclass(plugin_class, InputDatasetPluginBase):
                    self.register_plugin(plugin_class)
                    discovered += 1
                else:
                    logger.warning(
                        "Entry point '%s' does not inherit from %s",
                        ep.name,
                        InputDatasetPluginBase.__name__,
                    )
            except Exception as e:
                logger.error("Failed to load plugin %s: %s", ep.name, e)

        return discovered


# Global registry instance
_registry = InputDatasetPluginRegistry()


def get_registry() -> InputDatasetPluginRegistry:
    """Get the global input dataset plugin registry."""
    return _registry


def discover_plugins() -> int:
    """Discover and register plugins from packages."""
    return _registry.discover_plugins()
