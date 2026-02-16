"""Tests for the production input dataset plugin registry.

This module tests plugin registration, discovery, and instantiation
for the input dataset plugin system.
"""

from pathlib import Path
from typing import Any, ClassVar

import pytest

from dirac_cwl.production.core import (
    InputDatasetPluginBase,
    ProductionHint,
)
from dirac_cwl.production.registry import (
    InputDatasetPluginRegistry,
    get_registry,
)


class TestPlugin(InputDatasetPluginBase):
    """Test plugin for registry testing."""

    vo: ClassVar[str] = "generic"
    description: ClassVar[str] = "Test plugin for unit tests"

    def generate_inputs(
        self,
        workflow_path: Path,
        config: dict[str, Any],
        output_dir: Path,
        n_lfns: int | None = None,
        pick_smallest: bool = False,
    ) -> tuple[Path | None, Path | None]:
        """Generate test inputs (returns None for testing)."""
        return None, None


class TestVOPlugin(InputDatasetPluginBase):
    """Test vo-specific plugin."""

    vo: ClassVar[str] = "lhcb"
    description: ClassVar[str] = "Test LHCb plugin"

    def generate_inputs(
        self,
        workflow_path: Path,
        config: dict[str, Any],
        output_dir: Path,
        n_lfns: int | None = None,
        pick_smallest: bool = False,
    ) -> tuple[Path | None, Path | None]:
        """Generate test inputs (returns None for testing)."""
        return None, None


class TestSecondVOPlugin(InputDatasetPluginBase):
    """Test plugin for second vo."""

    vo: ClassVar[str] = "ctao"
    description: ClassVar[str] = "Test CTAO plugin"

    def generate_inputs(
        self,
        workflow_path: Path,
        config: dict[str, Any],
        output_dir: Path,
        n_lfns: int | None = None,
        pick_smallest: bool = False,
    ) -> tuple[Path | None, Path | None]:
        """Generate test inputs (returns None for testing)."""
        return None, None


class TestInputDatasetPluginRegistry:
    """Test the InputDatasetPluginRegistry class."""

    def test_creation(self):
        """Test registry creation."""
        registry = InputDatasetPluginRegistry()
        assert len(registry.list_plugins()) == 0
        assert len(registry.list_virtual_organizations()) == 0

    def test_register_plugin(self):
        """Test plugin registration."""
        registry = InputDatasetPluginRegistry()

        registry.register_plugin(TestPlugin)

        plugins = registry.list_plugins()
        assert "TestPlugin" in plugins
        assert len(plugins) == 1

    def test_register_plugin_with_vo(self):
        """Test vo-specific plugin registration."""
        registry = InputDatasetPluginRegistry()

        registry.register_plugin(TestVOPlugin)

        plugins = registry.list_plugins()
        assert "TestVOPlugin" in plugins

        vos = registry.list_virtual_organizations()
        assert "lhcb" in vos

        lhcb_plugins = registry.list_plugins(vo="lhcb")
        assert "TestVOPlugin" in lhcb_plugins

    def test_register_duplicate_plugin(self):
        """Test that duplicate registration raises error."""
        registry = InputDatasetPluginRegistry()

        registry.register_plugin(TestPlugin)

        with pytest.raises(ValueError, match="already registered"):
            registry.register_plugin(TestPlugin)

    def test_register_duplicate_plugin_with_override(self):
        """Test that duplicate registration works with override."""
        registry = InputDatasetPluginRegistry()

        registry.register_plugin(TestPlugin)
        registry.register_plugin(TestPlugin, override=True)

        plugins = registry.list_plugins()
        assert "TestPlugin" in plugins

    def test_register_invalid_class(self):
        """Test that registering non-plugin class raises error."""
        registry = InputDatasetPluginRegistry()

        class NotAPlugin:
            pass

        with pytest.raises(ValueError, match="must inherit from InputDatasetPluginBase"):
            registry.register_plugin(NotAPlugin)  # type: ignore

    def test_get_plugin(self):
        """Test getting registered plugin."""
        registry = InputDatasetPluginRegistry()

        registry.register_plugin(TestPlugin)

        plugin_class = registry.get_plugin("TestPlugin")
        assert plugin_class is TestPlugin

    def test_get_nonexistent_plugin(self):
        """Test getting non-existent plugin."""
        registry = InputDatasetPluginRegistry()

        plugin = registry.get_plugin("NonExistent")
        assert plugin is None

    def test_get_plugin_with_vo(self):
        """Test getting vo-specific plugin."""
        registry = InputDatasetPluginRegistry()

        registry.register_plugin(TestVOPlugin)

        plugin_class = registry.get_plugin("TestVOPlugin", vo="lhcb")
        assert plugin_class is TestVOPlugin

    def test_instantiate_plugin(self):
        """Test plugin instantiation."""
        registry = InputDatasetPluginRegistry()

        registry.register_plugin(TestPlugin)

        hint = ProductionHint(input_dataset_plugin="TestPlugin")
        instance = registry.instantiate(hint)

        assert isinstance(instance, TestPlugin)

    def test_instantiate_nonexistent_plugin(self):
        """Test instantiation of non-existent plugin."""
        registry = InputDatasetPluginRegistry()

        hint = ProductionHint(input_dataset_plugin="NonExistent")

        with pytest.raises(KeyError, match="Unknown input dataset plugin"):
            registry.instantiate(hint)

    def test_instantiate_no_plugin_specified(self):
        """Test instantiation with no plugin specified."""
        registry = InputDatasetPluginRegistry()

        hint = ProductionHint()  # No input_dataset_plugin

        with pytest.raises(ValueError, match="No input_dataset_plugin specified"):
            registry.instantiate(hint)

    def test_list_virtual_organizations(self):
        """Test listing vos."""
        registry = InputDatasetPluginRegistry()

        assert len(registry.list_virtual_organizations()) == 0

        registry.register_plugin(TestVOPlugin)  # lhcb
        registry.register_plugin(TestSecondVOPlugin)  # ctao

        vos = registry.list_virtual_organizations()
        assert "lhcb" in vos
        assert "ctao" in vos
        assert len(vos) == 2

    def test_list_plugins_by_vo(self):
        """Test listing plugins by vo."""
        registry = InputDatasetPluginRegistry()

        registry.register_plugin(TestPlugin)  # generic vo
        registry.register_plugin(TestVOPlugin)  # lhcb vo

        # All plugins
        all_plugins = registry.list_plugins()
        assert "TestPlugin" in all_plugins
        assert "TestVOPlugin" in all_plugins

        # vo-specific plugins
        lhcb_plugins = registry.list_plugins(vo="lhcb")
        assert "TestVOPlugin" in lhcb_plugins
        assert "TestPlugin" not in lhcb_plugins

    def test_get_plugin_info(self):
        """Test getting plugin info."""
        registry = InputDatasetPluginRegistry()

        registry.register_plugin(TestPlugin)

        info = registry.get_plugin_info("TestPlugin")
        assert info is not None
        assert info["plugin_name"] == "TestPlugin"
        assert info["vo"] == "generic"

    def test_get_plugin_info_nonexistent(self):
        """Test getting info for non-existent plugin."""
        registry = InputDatasetPluginRegistry()

        info = registry.get_plugin_info("NonExistent")
        assert info is None

    def test_discover_no_plugins(self, mocker, monkeypatch):
        """Test plugin discovery with no entry points."""
        from dirac_cwl.production import registry as registry_module

        # Mock entry_points to return empty
        monkeypatch.setattr(
            registry_module,
            "entry_points",
            lambda *args, **kwargs: [],
        )

        registry = InputDatasetPluginRegistry()
        discovered = registry.discover_plugins()
        assert discovered == 0

    def test_discover_plugins(self, mocker, monkeypatch):
        """Test plugin discovery from entry points."""
        from dirac_cwl.production import registry as registry_module

        class FakePlugin(InputDatasetPluginBase):
            def generate_inputs(
                self,
                workflow_path: Path,
                config: dict[str, Any],
                output_dir: Path,
                n_lfns: int | None = None,
                pick_smallest: bool = False,
            ) -> tuple[Path | None, Path | None]:
                return None, None

        class FakeWrongPlugin:
            pass

        # Create mock entry points
        fake_ep = mocker.MagicMock()
        fake_ep.load.return_value = FakePlugin

        wrong_ep = mocker.MagicMock()
        wrong_ep.name = "FakeWrongPlugin"
        wrong_ep.load.return_value = FakeWrongPlugin

        logger_mock = mocker.MagicMock()

        monkeypatch.setattr(
            registry_module,
            "entry_points",
            lambda *args, **kwargs: [fake_ep, wrong_ep],
        )
        monkeypatch.setattr(registry_module, "logger", logger_mock)

        registry = InputDatasetPluginRegistry()
        discovered = registry.discover_plugins()

        # Only FakePlugin should be discovered
        assert discovered == 1
        # Warning should have been logged for FakeWrongPlugin
        logger_mock.warning.assert_called_once()


class TestGlobalRegistryFunctions:
    """Test the global registry functions."""

    def test_get_registry(self):
        """Test getting the global registry."""
        registry = get_registry()
        assert isinstance(registry, InputDatasetPluginRegistry)

        # Should return the same instance
        registry2 = get_registry()
        assert registry is registry2
