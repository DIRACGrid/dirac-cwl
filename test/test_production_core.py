"""Tests for the production plugin core classes.

This module tests the ProductionHint and InputDatasetPluginBase classes.
"""

from pathlib import Path
from typing import Any, ClassVar

import pytest

from dirac_cwl.production.core import (
    InputDatasetPluginBase,
    ProductionHint,
)


class TestInputDatasetPluginBase:
    """Test the InputDatasetPluginBase class."""

    def test_abstract_class_cannot_be_instantiated(self):
        """Test that abstract base class cannot be instantiated directly."""
        with pytest.raises(TypeError, match="abstract"):
            InputDatasetPluginBase()

    def test_subclass_must_implement_generate_inputs(self):
        """Test that subclasses must implement generate_inputs."""

        class IncompletePlugin(InputDatasetPluginBase):
            pass

        with pytest.raises(TypeError, match="abstract"):
            IncompletePlugin()

    def test_concrete_subclass_can_be_instantiated(self):
        """Test that a complete subclass can be instantiated."""

        class ConcretePlugin(InputDatasetPluginBase):
            def generate_inputs(
                self,
                workflow_path: Path,
                config: dict[str, Any],
                output_dir: Path,
                n_lfns: int | None = None,
                pick_smallest: bool = False,
            ) -> tuple[Path | None, Path | None]:
                return None, None

        plugin = ConcretePlugin()
        assert plugin is not None

    def test_name_returns_class_name(self):
        """Test that name() returns the class name."""

        class MyCustomPlugin(InputDatasetPluginBase):
            def generate_inputs(
                self,
                workflow_path: Path,
                config: dict[str, Any],
                output_dir: Path,
                n_lfns: int | None = None,
                pick_smallest: bool = False,
            ) -> tuple[Path | None, Path | None]:
                return None, None

        assert MyCustomPlugin.name() == "MyCustomPlugin"

    def test_class_vars_have_defaults(self):
        """Test that class variables have default values."""

        class MinimalPlugin(InputDatasetPluginBase):
            def generate_inputs(
                self,
                workflow_path: Path,
                config: dict[str, Any],
                output_dir: Path,
                n_lfns: int | None = None,
                pick_smallest: bool = False,
            ) -> tuple[Path | None, Path | None]:
                return None, None

        assert MinimalPlugin.vo == "generic"
        assert MinimalPlugin.version == "1.0.0"
        assert MinimalPlugin.description == "Base input dataset plugin"

    def test_class_vars_can_be_overridden(self):
        """Test that class variables can be overridden."""

        class CustomPlugin(InputDatasetPluginBase):
            vo: ClassVar[str] = "lhcb"
            version: ClassVar[str] = "2.0.0"
            description: ClassVar[str] = "Custom plugin"

            def generate_inputs(
                self,
                workflow_path: Path,
                config: dict[str, Any],
                output_dir: Path,
                n_lfns: int | None = None,
                pick_smallest: bool = False,
            ) -> tuple[Path | None, Path | None]:
                return None, None

        assert CustomPlugin.vo == "lhcb"
        assert CustomPlugin.version == "2.0.0"
        assert CustomPlugin.description == "Custom plugin"

    def test_format_hint_display_default(self):
        """Test that format_hint_display returns empty list by default."""

        class MinimalPlugin(InputDatasetPluginBase):
            def generate_inputs(
                self,
                workflow_path: Path,
                config: dict[str, Any],
                output_dir: Path,
                n_lfns: int | None = None,
                pick_smallest: bool = False,
            ) -> tuple[Path | None, Path | None]:
                return None, None

        plugin = MinimalPlugin()
        result = plugin.format_hint_display({"key": "value"})
        assert result == []

    def test_get_schema_info(self):
        """Test get_schema_info returns correct information."""

        class InfoPlugin(InputDatasetPluginBase):
            vo: ClassVar[str] = "ctao"
            version: ClassVar[str] = "3.0.0"
            description: ClassVar[str] = "Info test plugin"

            def generate_inputs(
                self,
                workflow_path: Path,
                config: dict[str, Any],
                output_dir: Path,
                n_lfns: int | None = None,
                pick_smallest: bool = False,
            ) -> tuple[Path | None, Path | None]:
                return None, None

        info = InfoPlugin.get_schema_info()
        assert info["plugin_name"] == "InfoPlugin"
        assert info["vo"] == "ctao"
        assert info["version"] == "3.0.0"
        assert info["description"] == "Info test plugin"


class TestProductionHint:
    """Test the ProductionHint model."""

    def test_default_values(self):
        """Test default values."""
        hint = ProductionHint()
        assert hint.input_dataset_plugin is None
        assert hint.input_dataset_config == {}

    def test_with_plugin_name(self):
        """Test creating hint with plugin name."""
        hint = ProductionHint(input_dataset_plugin="LHCbBookkeepingPlugin")
        assert hint.input_dataset_plugin == "LHCbBookkeepingPlugin"

    def test_with_config(self):
        """Test creating hint with config."""
        config = {
            "event_type": "27165175",
            "conditions_dict": {"configName": "Collision24"},
        }
        hint = ProductionHint(
            input_dataset_plugin="LHCbBookkeepingPlugin",
            input_dataset_config=config,
        )
        assert hint.input_dataset_config == config

    def test_from_cwl_no_hints(self):
        """Test from_cwl with no hints."""
        cwl_content = {
            "cwlVersion": "v1.2",
            "class": "Workflow",
        }
        hint = ProductionHint.from_cwl(cwl_content)
        assert hint is None

    def test_from_cwl_no_production_hint(self):
        """Test from_cwl with hints but no dirac:Production."""
        cwl_content = {
            "cwlVersion": "v1.2",
            "class": "Workflow",
            "hints": [
                {"class": "dirac:Scheduling", "platform": "Linux"},
            ],
        }
        hint = ProductionHint.from_cwl(cwl_content)
        assert hint is None

    def test_from_cwl_with_production_hint(self):
        """Test from_cwl with dirac:Production hint."""
        cwl_content = {
            "cwlVersion": "v1.2",
            "class": "Workflow",
            "hints": [
                {
                    "class": "dirac:Production",
                    "input_dataset_plugin": "LHCbBookkeepingPlugin",
                    "input_dataset_config": {
                        "event_type": "27165175",
                    },
                },
            ],
        }
        hint = ProductionHint.from_cwl(cwl_content)
        assert hint is not None
        assert hint.input_dataset_plugin == "LHCbBookkeepingPlugin"
        assert hint.input_dataset_config["event_type"] == "27165175"

    def test_from_cwl_multiple_hints(self):
        """Test from_cwl with multiple hints including Production."""
        cwl_content = {
            "cwlVersion": "v1.2",
            "class": "Workflow",
            "hints": [
                {"class": "dirac:Scheduling", "platform": "Linux"},
                {
                    "class": "dirac:Production",
                    "input_dataset_plugin": "TestPlugin",
                },
                {"class": "dirac:ExecutionHooks", "hook_plugin": "Default"},
            ],
        }
        hint = ProductionHint.from_cwl(cwl_content)
        assert hint is not None
        assert hint.input_dataset_plugin == "TestPlugin"

    def test_from_cwl_empty_hints_list(self):
        """Test from_cwl with empty hints list."""
        cwl_content = {
            "cwlVersion": "v1.2",
            "class": "Workflow",
            "hints": [],
        }
        hint = ProductionHint.from_cwl(cwl_content)
        assert hint is None

    def test_allows_extra_fields(self):
        """Test that ProductionHint allows extra fields."""
        hint = ProductionHint(
            input_dataset_plugin="Test",
            some_extra_field="value",
        )
        assert hint.input_dataset_plugin == "Test"
        # Extra field should be accessible via model_extra
        assert "some_extra_field" in hint.model_extra
