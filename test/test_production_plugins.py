"""Tests for the built-in production input dataset plugins.

This module tests the NoOpInputDatasetPlugin and LHCbBookkeepingPlugin.
"""

from unittest.mock import MagicMock, patch

import pytest

from dirac_cwl.production.plugins.core import NoOpInputDatasetPlugin
from dirac_cwl.production.plugins.lhcb import LHCbBookkeepingPlugin


class TestNoOpInputDatasetPlugin:
    """Test the NoOpInputDatasetPlugin class."""

    def test_instantiation(self):
        """Test plugin can be instantiated."""
        plugin = NoOpInputDatasetPlugin()
        assert plugin is not None

    def test_class_vars(self):
        """Test class variables."""
        assert NoOpInputDatasetPlugin.vo == "generic"
        assert NoOpInputDatasetPlugin.version == "1.0.0"
        assert "No-operation" in NoOpInputDatasetPlugin.description

    def test_name(self):
        """Test plugin name."""
        assert NoOpInputDatasetPlugin.name() == "NoOpInputDatasetPlugin"

    def test_generate_inputs_returns_none(self, tmp_path):
        """Test that generate_inputs returns None for both values."""
        plugin = NoOpInputDatasetPlugin()

        inputs, catalog = plugin.generate_inputs(
            workflow_path=tmp_path / "workflow.cwl",
            config={},
            output_dir=tmp_path,
            n_lfns=10,
            pick_smallest=True,
        )

        assert inputs is None
        assert catalog is None

    def test_format_hint_display_returns_empty(self):
        """Test that format_hint_display returns empty list."""
        plugin = NoOpInputDatasetPlugin()
        result = plugin.format_hint_display({"key": "value"})
        assert result == []


class TestLHCbBookkeepingPlugin:
    """Test the LHCbBookkeepingPlugin class."""

    def test_class_vars(self):
        """Test class variables."""
        assert LHCbBookkeepingPlugin.vo == "lhcb"
        assert LHCbBookkeepingPlugin.version == "1.0.0"
        assert "LHCb" in LHCbBookkeepingPlugin.description

    def test_name(self):
        """Test plugin name."""
        assert LHCbBookkeepingPlugin.name() == "LHCbBookkeepingPlugin"

    def test_format_hint_display_with_event_type(self):
        """Test format_hint_display with event_type."""
        plugin = LHCbBookkeepingPlugin()
        config = {"event_type": "27165175"}

        result = plugin.format_hint_display(config)

        assert ("EventType", "27165175") in result

    def test_format_hint_display_with_conditions(self):
        """Test format_hint_display with conditions_description."""
        plugin = LHCbBookkeepingPlugin()
        config = {
            "conditions_description": "Beam6800GeV-VeloClosed-MagUp",
        }

        result = plugin.format_hint_display(config)

        assert ("Conditions", "Beam6800GeV-VeloClosed-MagUp") in result

    def test_format_hint_display_with_conditions_dict(self):
        """Test format_hint_display with conditions_dict fields."""
        plugin = LHCbBookkeepingPlugin()
        config = {
            "conditions_dict": {
                "configName": "Collision24",
                "inFileType": "BRUNELHLT2.DST",
                "inProPass": "Sprucing24c5/DaVinciRestripping25r0",
            },
        }

        result = plugin.format_hint_display(config)

        assert ("Config", "Collision24") in result
        assert ("FileType", "BRUNELHLT2.DST") in result
        assert ("ProcessingPass", "Sprucing24c5/DaVinciRestripping25r0") in result

    def test_format_hint_display_full_config(self):
        """Test format_hint_display with full config."""
        plugin = LHCbBookkeepingPlugin()
        config = {
            "event_type": "27165175",
            "conditions_description": "Beam6800GeV-VeloClosed-MagUp",
            "conditions_dict": {
                "configName": "Collision24",
                "inFileType": "BRUNELHLT2.DST",
                "inProPass": "Sprucing24c5",
            },
        }

        result = plugin.format_hint_display(config)

        # Should have all the fields
        keys = [item[0] for item in result]
        assert "EventType" in keys
        assert "Conditions" in keys
        assert "Config" in keys
        assert "FileType" in keys
        assert "ProcessingPass" in keys

    def test_format_hint_display_empty_config(self):
        """Test format_hint_display with empty config."""
        plugin = LHCbBookkeepingPlugin()
        result = plugin.format_hint_display({})
        assert result == []

    @patch("dirac_cwl.production.plugins.lhcb.importlib.util.find_spec")
    def test_generate_inputs_module_not_found(self, mock_find_spec, tmp_path):
        """Test generate_inputs raises error when module not found."""
        mock_find_spec.return_value = None

        plugin = LHCbBookkeepingPlugin()

        with pytest.raises(RuntimeError, match="Could not find"):
            plugin.generate_inputs(
                workflow_path=tmp_path / "workflow.cwl",
                config={},
                output_dir=tmp_path,
            )

    @patch("dirac_cwl.production.plugins.lhcb.subprocess.run")
    @patch("dirac_cwl.production.plugins.lhcb.importlib.util.find_spec")
    def test_generate_inputs_success(self, mock_find_spec, mock_run, tmp_path):
        """Test generate_inputs succeeds when subprocess succeeds."""
        # Mock the module spec
        mock_spec = MagicMock()
        mock_spec.origin = "/path/to/generate_replica_map.py"
        mock_find_spec.return_value = mock_spec

        # Mock successful subprocess run
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Generated successfully",
            stderr="",
        )

        plugin = LHCbBookkeepingPlugin()
        workflow_path = tmp_path / "workflow.cwl"
        workflow_path.touch()

        inputs, catalog = plugin.generate_inputs(
            workflow_path=workflow_path,
            config={},
            output_dir=tmp_path,
        )

        # Should return the expected paths
        assert inputs == tmp_path / "workflow-inputs.yml"
        assert catalog == tmp_path / "workflow-replica-map.json"

        # Verify subprocess was called with correct args
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "lb-dirac" in call_args
        assert "python" in call_args
        assert str(workflow_path) in call_args

    @patch("dirac_cwl.production.plugins.lhcb.subprocess.run")
    @patch("dirac_cwl.production.plugins.lhcb.importlib.util.find_spec")
    def test_generate_inputs_with_n_lfns(self, mock_find_spec, mock_run, tmp_path):
        """Test generate_inputs passes n_lfns parameter."""
        mock_spec = MagicMock()
        mock_spec.origin = "/path/to/generate_replica_map.py"
        mock_find_spec.return_value = mock_spec

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        plugin = LHCbBookkeepingPlugin()
        workflow_path = tmp_path / "workflow.cwl"
        workflow_path.touch()

        plugin.generate_inputs(
            workflow_path=workflow_path,
            config={},
            output_dir=tmp_path,
            n_lfns=5,
        )

        call_args = mock_run.call_args[0][0]
        assert "--n-lfns" in call_args
        assert "5" in call_args

    @patch("dirac_cwl.production.plugins.lhcb.subprocess.run")
    @patch("dirac_cwl.production.plugins.lhcb.importlib.util.find_spec")
    def test_generate_inputs_with_pick_smallest(self, mock_find_spec, mock_run, tmp_path):
        """Test generate_inputs passes pick_smallest flag."""
        mock_spec = MagicMock()
        mock_spec.origin = "/path/to/generate_replica_map.py"
        mock_find_spec.return_value = mock_spec

        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        plugin = LHCbBookkeepingPlugin()
        workflow_path = tmp_path / "workflow.cwl"
        workflow_path.touch()

        plugin.generate_inputs(
            workflow_path=workflow_path,
            config={},
            output_dir=tmp_path,
            pick_smallest=True,
        )

        call_args = mock_run.call_args[0][0]
        assert "--pick-smallest-lfn" in call_args

    @patch("dirac_cwl.production.plugins.lhcb.subprocess.run")
    @patch("dirac_cwl.production.plugins.lhcb.importlib.util.find_spec")
    def test_generate_inputs_failure(self, mock_find_spec, mock_run, tmp_path):
        """Test generate_inputs raises error on subprocess failure."""
        mock_spec = MagicMock()
        mock_spec.origin = "/path/to/generate_replica_map.py"
        mock_find_spec.return_value = mock_spec

        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Error: Bookkeeping query failed",
        )

        plugin = LHCbBookkeepingPlugin()
        workflow_path = tmp_path / "workflow.cwl"
        workflow_path.touch()

        with pytest.raises(RuntimeError, match="failed with exit code 1"):
            plugin.generate_inputs(
                workflow_path=workflow_path,
                config={},
                output_dir=tmp_path,
            )
