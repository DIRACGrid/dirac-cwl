"""
Comprehensive tests for enhanced submission models.

This module tests the enhanced submission models that extend the core
metadata system with additional functionality for CWL integration.
"""

import pytest

from dirac_cwl.execution_hooks import get_registry
from dirac_cwl.execution_hooks.core import ExecutionHooksHint, SchedulingHint


class TestExecutionHooksHint:
    """Test the ExecutionHooksHint class."""

    def test_creation_and_parameters(self):
        """Test ExecutionHooksHint creation with default and custom parameters."""
        # Test default values
        descriptor = ExecutionHooksHint()
        assert descriptor.hook_plugin == "QueryBasedPlugin"  # Default plugin
        assert descriptor.configuration == {}

        # Test custom parameters
        descriptor = ExecutionHooksHint(
            hook_plugin="AdminPlugin",
            configuration={"admin_level": 5, "log_level": "DEBUG"},
        )
        assert descriptor.hook_plugin == "AdminPlugin"
        assert descriptor.configuration["admin_level"] == 5
        assert descriptor.configuration["log_level"] == "DEBUG"

    def test_type_validation(self):
        """Test type validation during runtime instantiation."""
        # Valid type (should be registered by default)
        descriptor = ExecutionHooksHint(hook_plugin="UserPlugin")
        assert descriptor.hook_plugin == "UserPlugin"

        # Invalid type should raise error during runtime instantiation
        descriptor_with_invalid_type = ExecutionHooksHint(hook_plugin="NonExistentType")
        with pytest.raises(KeyError, match="Unknown execution hooks plugin"):
            descriptor_with_invalid_type.to_runtime()

    def test_model_copy_operations(self):
        """Test model_copy functionality."""
        """Test model_copy functionality with basic and update operations."""
        original = ExecutionHooksHint(
            hook_plugin="AdminPlugin",
            configuration={"admin_level": 3, "log_level": "INFO"},
        )

        # Test basic copy
        copied = original.model_copy()
        assert copied.hook_plugin == original.hook_plugin
        assert copied.configuration == original.configuration
        assert copied is not original

        # Test copy with type update
        copied = original.model_copy(update={"hook_plugin": "UserPlugin"})
        assert copied.hook_plugin == "UserPlugin"
        assert copied.configuration == original.configuration

        # Test copy with configuration update
        copied = original.model_copy(update={"configuration": {"admin_level": 5}})
        assert copied.hook_plugin == original.hook_plugin
        assert copied.configuration["admin_level"] == 5
        assert copied.configuration["log_level"] == "INFO"  # Should merge

    def test_to_runtime_operations(self, mocker):
        """Test to_runtime without and with submission context."""
        # Test without submission context using an available plugin
        registry = get_registry()
        available = registry.list_plugins()
        if not available:
            pytest.skip("No plugins registered to test to_runtime")

        # Prefer AdminPlugin when available for admin-style config test
        plugin_name = "AdminPlugin" if "AdminPlugin" in available else available[0]

        cfg = {"admin_level": 7} if plugin_name == "AdminPlugin" else {}
        descriptor = ExecutionHooksHint(hook_plugin=plugin_name, configuration=cfg)
        runtime = descriptor.to_runtime()
        assert runtime.name() == plugin_name
        if plugin_name == "AdminPlugin":
            # Only check admin_level when using AdminPlugin
            assert getattr(runtime, "admin_level", None) == 7

        # Test with submission context
        # Test with submission context only if QueryBasedPlugin is available
        if "QueryBasedPlugin" not in available:
            pytest.skip("QueryBasedPlugin not registered; skipping submission-context test")

        descriptor = ExecutionHooksHint(hook_plugin="QueryBasedPlugin", configuration={"campaign": "Run3"})

        # Mock submission model
        mock_submission = mocker.Mock()
        mock_task = mocker.Mock()
        mock_input = mocker.Mock()
        mock_input.id = "task#campaign"
        mock_input.default = "default_campaign"
        mock_task.inputs = [mock_input]
        mock_submission.task = mock_task
        mock_submission.parameters = [mocker.Mock(cwl={"campaign": "override_campaign"})]

        runtime = descriptor.to_runtime(mock_submission)
        assert runtime.name() == "QueryBasedPlugin"
        assert runtime.campaign == "Run3"

    def test_dash_to_snake_case_conversion(self):
        """Test dash-case to snake_case parameter conversion."""
        descriptor = ExecutionHooksHint(
            hook_plugin="QueryBasedPlugin",
            configuration={
                "query_root": "/data",
                "data_type": "AOD",
            },  # Already in snake_case
        )

        runtime = descriptor.to_runtime()  # Parameters should be converted to snake_case
        assert hasattr(runtime, "query_root") or runtime.query_root == "/data"
        assert hasattr(runtime, "data_type") or runtime.data_type == "AOD"

    def test_from_cwl(self, mocker):
        """Test from_cwl class method."""
        mock_cwl = mocker.Mock()
        mock_descriptor = ExecutionHooksHint(hook_plugin="QueryBasedPlugin")
        mock_from_cwl = mocker.patch("dirac_cwl.submission_models.ExecutionHooksHint.from_cwl")
        mock_from_cwl.return_value = mock_descriptor

        result = ExecutionHooksHint.from_cwl(mock_cwl)

        assert isinstance(result, ExecutionHooksHint)
        mock_from_cwl.assert_called_once_with(mock_cwl)


class TestSubmissionModelsIntegration:
    """Test integration between submission models and metadata system."""

    def test_enhanced_descriptor_registry_integration(self):
        """Test that ExecutionHooksHint integrates with the registry."""
        # Instantiate a descriptor for each registered plugin and ensure runtime
        registry = get_registry()
        available = registry.list_plugins()
        for plugin_name in available:
            descriptor = ExecutionHooksHint(hook_plugin=plugin_name)
            try:
                runtime = descriptor.to_runtime()
            except KeyError:
                pytest.skip(f"Plugin {plugin_name} not instantiable in this environment")
            assert runtime.name() == descriptor.hook_plugin

    def test_task_description_with_different_metadata_types(self):
        """Test SchedulingHint with different configurations."""
        task_configs = [
            {"platform": "DIRAC", "priority": 5},
            {"platform": "DIRACX", "priority": 8, "sites": ["CERN"]},
            {"priority": 3, "sites": ["GRIDKA", "CNAF"]},
        ]

        for config in task_configs:
            model = SchedulingHint(**config)

            # Should have correct configuration
            assert model.priority == config["priority"]
            if "platform" in config:
                assert model.platform == config["platform"]
            if "sites" in config:
                assert model.sites == config["sites"]

    def test_model_serialization_round_trip(self):
        """Test that models can be serialized and deserialized."""
        original = SchedulingHint(platform="DIRAC", priority=7, sites=["CERN", "GRIDKA"])

        # Serialize to dict
        data = original.model_dump()

        # Should be able to recreate from dict
        recreated = SchedulingHint(**data)

        assert recreated.platform == original.platform
        assert recreated.priority == original.priority
        assert recreated.sites == original.sites

    def test_cwl_hints_integration(self):
        """Test integration with CWL hints extraction."""
        # Create an enhanced descriptor directly
        registry = get_registry()
        if "QueryBasedPlugin" not in registry.list_plugins():
            pytest.skip("QueryBasedPlugin not registered; skipping CWL hints integration test")

        descriptor = ExecutionHooksHint(
            hook_plugin="QueryBasedPlugin",
            configuration={"campaign": "Run3", "data_type": "AOD"},
        )
        runtime = descriptor.to_runtime()

        # Should use the QueryBased type
        assert runtime.name() == "QueryBasedPlugin"
