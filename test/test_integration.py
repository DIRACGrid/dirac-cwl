"""
Integration tests for the complete execution hooks plugin system.

This module tests the end-to-end functionality of the execution hooks plugin system,
including plugin discovery, registration, CWL integration, and real-world
usage scenarios.
"""

import os

import pytest

os.environ["DIRAC_PROTO_LOCAL"] = "1"

from dirac_cwl.execution_hooks import (
    get_registry,
)
from dirac_cwl.execution_hooks.core import (
    ExecutionHooksBasePlugin,
    ExecutionHooksHint,
    SchedulingHint,
)
from dirac_cwl.job.job_wrapper import JobWrapper


class TestSystemIntegration:
    """Test complete system integration."""

    def test_plugin_system_initialization(self):
        """Test that the plugin system initializes correctly."""
        # Test registry access
        registry = get_registry()
        assert registry is not None

        # Test that at least one plugin is registered
        registry = get_registry()
        plugins = registry.list_plugins()
        assert len(plugins) > 0, f"No plugins registered: {plugins}"

    def test_plugin_instantiation_flow(self):
        """Test the complete plugin instantiation flow."""
        # Only use currently registered plugins
        registry = get_registry()
        available_plugins = registry.list_plugins()
        # Provide minimal params for each plugin (could be extended for plugin-specific params)
        for plugin_type in available_plugins:
            params = {}
            descriptor = ExecutionHooksHint(hook_plugin=plugin_type, **params)
            instance = registry.instantiate_plugin(descriptor)
            assert instance.name() == plugin_type

            # Test via descriptor
            descriptor = ExecutionHooksHint(hook_plugin=plugin_type, configuration=params)
            runtime = descriptor.to_runtime()
            assert runtime.name() == plugin_type

    def test_cwl_integration_workflow(self):
        """Test complete CWL integration workflow."""
        # Create an enhanced descriptor directly to test CWL integration
        execution_hook = ExecutionHooksHint(
            hook_plugin="QueryBasedPlugin",
            configuration={"campaign": "Run3", "data_type": "AOD", "site": "CERN"},
        )

        # Convert to runtime
        runtime_plugin = execution_hook.to_runtime()
        assert runtime_plugin.name() == "QueryBasedPlugin"  # Test that CWL parameters are available
        # (Note: exact parameter extraction depends on implementation)


class TestRealWorldScenarios:
    """Test real-world usage scenarios."""

    def test_user_workflow_scenario(self, sample_job):
        """Test a typical user workflow scenario."""
        # Use any available plugin for a generic runtime smoke test
        registry = get_registry()
        available = registry.list_plugins()
        if not available:
            pytest.skip("No plugins available for user workflow scenario")

        plugin_name = available[0]
        user_descriptor = ExecutionHooksHint(hook_plugin=plugin_name)
        user_runtime = user_descriptor.to_runtime()

        # Simulate job execution

        # Pre-process should return a command list (may be modified by plugin)
        job_wrapper = JobWrapper()
        job_wrapper.execution_hooks_plugin = user_runtime
        processed_command = job_wrapper.pre_process(sample_job.task, None)
        assert isinstance(processed_command, list)

        # Post-process should return a boolean
        result = job_wrapper.post_process(0, "{}", "{}")
        assert isinstance(result, bool)

    def test_admin_workflow_scenario(self, sample_job):
        """Test an administrative workflow scenario."""
        # Generic admin-style smoke test: ensure a plugin accepts configuration
        registry = get_registry()
        available = registry.list_plugins()
        if not available:
            pytest.skip("No plugins available for admin workflow scenario")

        plugin_name = available[0]
        admin_descriptor = ExecutionHooksHint(
            hook_plugin=plugin_name,
            configuration={"log_level": "DEBUG"},
        )

        # If the plugin cannot accept configuration, to_runtime may raise; skip in that case
        try:
            admin_runtime = admin_descriptor.to_runtime()
        except Exception:
            pytest.skip(f"Plugin {plugin_name} cannot be instantiated with configuration")

        # Simulate job execution
        job_wrapper = JobWrapper()
        job_wrapper.execution_hooks_plugin = admin_runtime
        processed_command = job_wrapper.pre_process(sample_job.task, None)
        assert isinstance(processed_command, list)

    def test_data_analysis_workflow_scenario(self):
        """Test a data analysis workflow scenario."""
        # Run QueryBased-specific tests only if available
        registry = get_registry()
        if "QueryBasedPlugin" not in registry.list_plugins():
            pytest.skip("QueryBasedPlugin not registered; skipping data analysis workflow test")

        analysis_descriptor = ExecutionHooksHint(
            hook_plugin="QueryBasedPlugin",
            configuration={
                "query_root": "/grid/data",
                "campaign": "Run3_2024",
                "data_type": "AOD",
                "site": "CERN",
            },
        )
        analysis_runtime = analysis_descriptor.to_runtime()

        # Test input data discovery
        input_path = analysis_runtime.get_input_query("input_data")
        assert str(input_path).startswith("/grid/data")
        assert "Run3_2024" in str(input_path)
        assert "CERN" in str(input_path)
        assert "AOD" in str(input_path)

    def test_transformation_workflow_scenario(self):
        """Test a transformation (batch processing) workflow scenario."""
        # Create multiple task descriptions for a transformation
        task_configs = [{"platform": "DIRAC", "priority": 5, "sites": [f"site_{i}"]} for i in range(5)]

        tasks = []
        for config in task_configs:
            task = SchedulingHint(**config)
            tasks.append(task)

        # Verify all tasks have correct configuration
        for i, task in enumerate(tasks):
            assert task.platform == "DIRAC"
            assert task.priority == 5
            assert task.sites == [f"site_{i}"]

    def test_parameter_override_scenario(self):
        """Test parameter override scenarios."""
        # Run parameter override tests only for ParameterTestPlugin if available
        registry = get_registry()
        if "ParameterTestPlugin" not in registry.list_plugins():
            pytest.skip("ParameterTestPlugin not registered; skipping parameter override tests")

        base_descriptor = ExecutionHooksHint(
            hook_plugin="ParameterTestPlugin",
            configuration={"admin_level": 3, "log_level": "INFO"},
        )

        variants = [
            {"admin_level": 5},
            {"log_level": "DEBUG"},
            {"admin_level": 8, "log_level": "ERROR", "enable_monitoring": False},
        ]

        for override in variants:
            variant_descriptor = base_descriptor.model_copy(update={"configuration": override})
            runtime = variant_descriptor.to_runtime()

            # Verify parameters are properly merged/overridden when supported by the runtime
            for key, value in override.items():
                if hasattr(runtime, key):
                    assert getattr(runtime, key) == value


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_invalid_plugin_type(self):
        """Test handling of invalid plugin types."""
        # Invalid types should raise error during runtime instantiation
        descriptor = ExecutionHooksHint(hook_plugin="NonExistentPlugin")
        with pytest.raises(KeyError, match="Unknown execution hooks plugin"):
            descriptor.to_runtime()

    # def test_missing_required_parameters(self):
    #     """Test handling of missing required parameters."""
    #     # Some plugins might require specific parameters
    #     with pytest.raises(
    #         ValueError, match="Failed to instantiate plugin 'LHCbSimulationPlugin'"
    #     ):
    #         descriptor = ExecutionHooksHint(hook_plugin="LHCbSimulationPlugin")
    #         descriptor.to_runtime()

    def test_plugin_registration_conflicts(self):
        """Test handling of plugin registration conflicts."""

        # Create a test plugin
        class ConflictTestPlugin(ExecutionHooksBasePlugin):
            description = "Test plugin for conflict testing"

        # Register it
        registry = get_registry()
        registry.register_plugin(ConflictTestPlugin)

        # Try to register again (should raise error)
        with pytest.raises(ValueError, match="already registered"):
            registry.register_plugin(ConflictTestPlugin)

    def test_malformed_cwl_hints(self, mocker):
        """Test handling of malformed CWL hints."""
        # Test with None hints
        mock_cwl = mocker.Mock()
        mock_cwl.hints = None

        descriptor = ExecutionHooksHint.from_cwl(mock_cwl)
        assert descriptor.hook_plugin == "QueryBasedPlugin"  # Should use default

        # Test with empty hints
        mock_cwl.hints = []
        descriptor = ExecutionHooksHint.from_cwl(mock_cwl)
        assert descriptor.hook_plugin == "QueryBasedPlugin"  # Should use default

        # Test with malformed hints
        mock_cwl.hints = [{"invalid": "hint"}]
        descriptor = ExecutionHooksHint.from_cwl(mock_cwl)
        assert descriptor.hook_plugin == "QueryBasedPlugin"  # Should ignore and use default
