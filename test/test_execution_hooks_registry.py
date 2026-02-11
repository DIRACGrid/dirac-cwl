"""
Tests for the execution hooks plugin registry system.

This module tests the plugin registration, discovery, and instantiation
functionality of the execution hooks registry.
"""

from pathlib import Path
from typing import Any, ClassVar, Optional

import pytest

from dirac_cwl.execution_hooks.core import (
    ExecutionHooksBasePlugin,
    ExecutionHooksHint,
)
from dirac_cwl.execution_hooks.registry import (
    ExecutionHooksPluginRegistry,
    get_registry,
)


class TestPlugin(ExecutionHooksBasePlugin):
    """Test plugin for registry testing."""

    description: ClassVar[str] = "Test plugin for unit tests"

    test_param: str = "default"


class TestVOPlugin(ExecutionHooksBasePlugin):
    """Test vo-specific plugin."""

    description: ClassVar[str] = "Test VO plugin"
    vo: ClassVar[Optional[str]] = "test_exp"

    exp_param: int = 42


class TestSecondVOPlugin(ExecutionHooksBasePlugin):
    """Test plugin for second vo."""

    description: ClassVar[str] = "Test plugin for second VO"
    vo: ClassVar[Optional[str]] = "exp2"

    param2: str = "test"


class TestExecutionHooksPluginRegistry:
    """Test the ExecutionHooksPluginRegistry class."""

    def test_creation(self):
        """Test registry creation."""
        registry = ExecutionHooksPluginRegistry()
        assert len(registry.list_plugins()) == 0
        assert len(registry.list_virtual_organizations()) == 0

    def test_register_plugin(self):
        """Test plugin registration."""
        registry = ExecutionHooksPluginRegistry()

        registry.register_plugin(TestPlugin)

        plugins = registry.list_plugins()
        assert "TestPlugin" in plugins
        assert len(plugins) == 1

    def test_register_plugin_with_vo(self):
        """Test vo-specific plugin registration."""
        registry = ExecutionHooksPluginRegistry()

        registry.register_plugin(TestVOPlugin)

        plugins = registry.list_plugins()
        assert "TestVOPlugin" in plugins

        vos = registry.list_virtual_organizations()
        assert "test_exp" in vos

        exp_plugins = registry.list_plugins(vo="test_exp")
        assert "TestVOPlugin" in exp_plugins

    def test_register_duplicate_plugin(self):
        """Test that duplicate registration raises error."""
        registry = ExecutionHooksPluginRegistry()

        registry.register_plugin(TestPlugin)

        # Should raise error without override
        with pytest.raises(ValueError, match="already registered"):
            registry.register_plugin(TestPlugin)

    def test_register_duplicate_plugin_with_override(self):
        """Test that duplicate registration works with override."""
        registry = ExecutionHooksPluginRegistry()

        registry.register_plugin(TestPlugin)
        registry.register_plugin(TestPlugin, override=True)

        # Should not raise error with override=True
        plugins = registry.list_plugins()
        assert "TestPlugin" in plugins

    def test_get_plugin(self):
        """Test getting registered plugin."""
        registry = ExecutionHooksPluginRegistry()

        registry.register_plugin(TestPlugin)

        plugin_class = registry.get_plugin("TestPlugin")
        assert plugin_class is TestPlugin

    def test_get_nonexistent_plugin(self):
        """Test getting non-existent plugin."""
        registry = ExecutionHooksPluginRegistry()

        plugin = registry.get_plugin("NonExistent")
        assert plugin is None

    def test_get_plugin_with_vo(self):
        """Test getting vo-specific plugin."""
        registry = ExecutionHooksPluginRegistry()

        registry.register_plugin(TestVOPlugin)

        plugin_class = registry.get_plugin("TestVOPlugin", vo="test_exp")
        assert plugin_class is TestVOPlugin

    def test_instantiate_plugin(self):
        """Test plugin instantiation."""
        registry = ExecutionHooksPluginRegistry()

        registry.register_plugin(TestPlugin)

        descriptor = ExecutionHooksHint(hook_plugin="TestPlugin", test_param="custom")
        instance = registry.instantiate_plugin(descriptor)

        assert isinstance(instance, TestPlugin)
        assert instance.test_param == "custom"

    def test_instantiate_plugin_with_vo(self):
        """Test vo plugin instantiation."""
        registry = ExecutionHooksPluginRegistry()

        registry.register_plugin(TestVOPlugin)

        descriptor = ExecutionHooksHint(hook_plugin="TestVOPlugin", vo="test_exp", exp_param=99)
        instance = registry.instantiate_plugin(descriptor)

        assert isinstance(instance, TestVOPlugin)
        assert instance.exp_param == 99

    def test_instantiate_nonexistent_plugin(self):
        """Test instantiation of non-existent plugin."""
        registry = ExecutionHooksPluginRegistry()

        descriptor = ExecutionHooksHint(hook_plugin="NonExistent")

        with pytest.raises(KeyError, match="Unknown execution hooks plugin"):
            registry.instantiate_plugin(descriptor)

    def test_discover_no_plugins(self, mocker, monkeypatch):
        """Test automatic plugin discovery."""
        from dirac_cwl.execution_hooks import registry

        # Mock entry_points to have nothing
        entrypointMock = mocker.MagicMock()
        entrypointMock.names = []
        monkeypatch.setattr(registry, "entry_points", lambda *args, **kwargs: entrypointMock)

        registry = ExecutionHooksPluginRegistry()

        # Test discovery with no hooks at the entrypoints - should return 0
        discovered = registry.discover_plugins()
        assert discovered == 0

        # Test that the registry still works normally
        plugins = registry.list_plugins()
        assert isinstance(plugins, list)

    def test_discover_plugins(self, mocker, monkeypatch):
        """Test plugin discovery from entry points."""
        from dirac_cwl.execution_hooks import registry

        class FakePlugin(ExecutionHooksBasePlugin): ...

        class FakeWrongPlugin: ...

        # Prepare the mocks
        entrypointMock = mocker.MagicMock()
        loggerMock = mocker.MagicMock()
        fakePluginMock = mocker.MagicMock()
        fakeWrongPluginMock = mocker.MagicMock()

        # Set the mocked entries
        mockedEntries = {
            "FakePlugin": fakePluginMock,
            "FakeWrongPlugin": fakeWrongPluginMock,
        }

        # Set the load() return values of the entrypoints
        fakePluginMock.load.return_value = FakePlugin
        fakeWrongPluginMock.load.return_value = FakeWrongPlugin

        # Prepare the mocked entry_point
        entrypointMock.names = list(mockedEntries.keys())
        entrypointMock.__getitem__.side_effect = mockedEntries.__getitem__

        # Patch both the entry_points and the logger
        monkeypatch.setattr(registry, "entry_points", lambda *args, **kwargs: entrypointMock)
        monkeypatch.setattr(registry, "logger", loggerMock)

        # Execute discover_plugins
        discovered = registry.discover_plugins()

        # It should have only discovered "FakePlugin"
        assert discovered == 1
        # "FakeWrongPlugin" should have logged a warning
        loggerMock.warning.assert_called_once()

    def test_list_virtual_organizations(self):
        """Test listing vos."""
        registry = ExecutionHooksPluginRegistry()

        assert len(registry.list_virtual_organizations()) == 0

        registry.register_plugin(TestVOPlugin)  # exp1 = test_exp
        registry.register_plugin(TestSecondVOPlugin)  # exp2

        vos = registry.list_virtual_organizations()
        assert "test_exp" in vos
        assert "exp2" in vos
        assert len(vos) == 2

    def test_list_plugins_by_vo(self):
        """Test listing plugins by vo."""
        registry = ExecutionHooksPluginRegistry()

        registry.register_plugin(TestPlugin)  # No vo
        registry.register_plugin(TestVOPlugin)  # test_exp vo

        # All plugins
        all_plugins = registry.list_plugins()
        assert "TestPlugin" in all_plugins
        assert "TestVOPlugin" in all_plugins

        # vo-specific plugins
        exp_plugins = registry.list_plugins(vo="test_exp")
        assert "TestVOPlugin" in exp_plugins
        assert "TestPlugin" not in exp_plugins


class TestGlobalRegistryFunctions:
    """Test the global registry functions."""

    def test_get_registry(self):
        """Test getting the global registry."""
        registry = get_registry()
        assert isinstance(registry, ExecutionHooksPluginRegistry)

        # Should return the same instance
        registry2 = get_registry()
        assert registry is registry2


class TestPluginSystem:
    """Test the modern plugin system functionality."""

    def test_direct_plugin_usage(self):
        """Test using plugins directly without legacy wrapper."""
        from dirac_cwl.execution_hooks.core import ExecutionHooksBasePlugin

        class DirectPlugin(ExecutionHooksBasePlugin):
            test_param: str = "default"

            def get_input_query(self, **kwargs: Any) -> Optional[Path]:
                return Path(f"/direct/{self.test_param}")

        # Register plugin directly
        registry = get_registry()
        registry.register_plugin(DirectPlugin)

        # Should be able to instantiate
        descriptor = ExecutionHooksHint(hook_plugin="DirectPlugin", test_param="custom")
        instance = registry.instantiate_plugin(descriptor)

        # Should work with new interface
        result = instance.get_input_query()
        assert result == Path("/direct/custom")

    def test_plugin_parameter_handling(self):
        """Test that parameters are passed correctly to plugins."""
        from dirac_cwl.execution_hooks.core import ExecutionHooksBasePlugin

        class ParameterTestPlugin(ExecutionHooksBasePlugin):
            test_param: str = "default"
            another_param: str = "default"

        registry = get_registry()
        registry.register_plugin(ParameterTestPlugin)

        # Test with snake_case parameters (should work)
        descriptor = ExecutionHooksHint(
            hook_plugin="ParameterTestPlugin",
            test_param="value1",
            another_param="value2",
        )
        instance = registry.instantiate_plugin(descriptor)

        assert instance.test_param == "value1"
        assert instance.another_param == "value2"
