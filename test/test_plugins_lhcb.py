"""
Tests for the LHCb experiment execution hooks plugins.

This module tests the LHCb-specific execution hooks plugins including simulation,
reconstruction, and analysis execution hooks implementations.
"""

from pathlib import Path

from dirac_cwl.execution_hooks.plugins.lhcb import (
    LHCbAnalysisPlugin,
    LHCbBasePlugin,
    LHCbReconstructionPlugin,
    LHCbSimulationPlugin,
)


class TestLHCbBasePlugin:
    """Test the base LHCbBasePlugin class."""

    def test_creation_and_path_generation(self):
        """Test LHCbBasePlugin creation and path generation."""
        plugin = LHCbBasePlugin(task_id=123, run_id=456)
        assert plugin.task_id == 123
        assert plugin.run_id == 456
        assert plugin.vo == "lhcb"

        # Test LHCb-specific path generation through data catalog interface
        from dirac_cwl.execution_hooks.plugins.lhcb import (
            LHCbDataCatalogInterface,
        )

        data_catalog = LHCbDataCatalogInterface(task_id=123, run_id=456)
        path = data_catalog.get_lhcb_base_path(task_id=12345, run_id=1)
        expected = Path("filecatalog/lhcb/12345/1")
        assert path == expected

    def test_get_input_query(self):
        """Test base get_input_query implementation."""
        plugin = LHCbBasePlugin(task_id=1, run_id=1)

        result = plugin.get_input_query("input_file")
        # Base LHCbBasePlugin returns None - only derived classes implement queries
        assert result is None

    def test_get_output_query(self):
        """Test base get_output_query implementation."""
        plugin = LHCbBasePlugin(task_id=2, run_id=2)

        result = plugin.get_output_query("output_file")
        # Base LHCbBasePlugin returns None - only derived classes implement queries
        assert result is None


class TestLHCbSimulationPlugin:
    """Test the LHCbSimulationPlugin class."""

    def test_creation_and_simulation_parameters(self):
        """Test LHCbSimulationPlugin creation with simulation-specific parameters."""
        # Test basic creation
        plugin = LHCbSimulationPlugin(task_id=123, run_id=1)
        assert plugin.name() == "LHCbSimulationPlugin"
        assert "LHCb simulation" in plugin.description

        # Test creation with simulation-specific parameters
        plugin = LHCbSimulationPlugin(
            task_id=123,
            run_id=1,
            detector_conditions="sim-20230101-vc-md100",
            beam_energy=6500.0,
            generator_config="Pythia8",
        )
        assert plugin.detector_conditions == "sim-20230101-vc-md100"
        assert plugin.beam_energy == 6500.0
        assert plugin.generator_config == "Pythia8"

    def test_pre_process_simulation(self):
        """Test simulation pre_process method."""
        plugin = LHCbSimulationPlugin(task_id=123, run_id=1, generator_config="Pythia8")

        command = ["lhcb-simulation", "workflow.cwl"]
        result = plugin.pre_process({}, None, Path("/tmp/job"), command)

        # The pre_process method calculates optimal events and updates parameters
        # It returns the original command unchanged
        assert isinstance(result, list)
        assert result[0] == "lhcb-simulation"
        assert result[1] == "workflow.cwl"

    def test_update_job_parameters(self, mocker):
        """Test job parameter updates."""
        plugin = LHCbSimulationPlugin(task_id=123, run_id=1)

        # Mock parameter file loading
        mock_load_inputfile = mocker.patch("dirac_cwl.execution_hooks.plugins.lhcb.load_inputfile")
        mock_open = mocker.patch("builtins.open")
        mock_yaml = mocker.patch("dirac_cwl.execution_hooks.plugins.lhcb.YAML")
        mock_load_inputfile.return_value = {"n_events": 1500, "generator": "Pythia8"}

        job_path = Path("/tmp/job")
        command = ["simulate.py", "--config", "params.yaml"]

        plugin._update_job_parameters(job_path, command)

        # Should have loaded parameters file
        mock_load_inputfile.assert_called_once()
        # Should have opened file for writing
        mock_open.assert_called_once()
        # Should have dumped YAML content
        mock_yaml.return_value.dump.assert_called_once()

    def test_update_job_parameters_file_error(self, mocker):
        """Test job parameter update with file error."""
        plugin = LHCbSimulationPlugin(task_id=123, run_id=1)

        # Mock file loading error
        mock_load_inputfile = mocker.patch("dirac_cwl.execution_hooks.plugins.lhcb.load_inputfile")
        mock_open = mocker.patch("builtins.open")
        mock_yaml = mocker.patch("dirac_cwl.execution_hooks.plugins.lhcb.YAML")
        mock_load_inputfile.side_effect = Exception("File not found")

        job_path = Path("/tmp/job")
        command = ["simulate.py", "--config", "params.yaml"]

        # Should handle error gracefully
        plugin._update_job_parameters(job_path, command)

        # Should still try to open file for writing
        mock_open.assert_called_once()
        # Should still dump YAML content with fallback parameters
        mock_yaml.return_value.dump.assert_called_once()

    def test_pre_process(self):
        """Test simulation pre_process method."""
        plugin = LHCbSimulationPlugin(task_id=123, run_id=1, generator_config="Pythia8")

        command = ["lhcb-simulation", "workflow.cwl"]
        result = plugin.pre_process({}, None, Path("/tmp/job"), command)

        # The pre_process method calculates optimal events and updates parameters
        # It returns the original command unchanged (actual implementation doesn't modify command)
        assert isinstance(result, list)
        assert result[0] == "lhcb-simulation"
        assert result[1] == "workflow.cwl"
        # The number of events is calculated and stored in the plugin
        assert plugin.number_of_events >= 0  # Should have calculated events

    def test_post_process(self, mocker):
        """Test simulation post_process method."""
        plugin = LHCbSimulationPlugin(task_id=123, run_id=1)

        job_path = Path("/tmp/job")

        # Mock glob to find simulation files
        mock_glob = mocker.patch("dirac_cwl.execution_hooks.plugins.lhcb.glob.glob")
        # Mock the store_output method
        mock_store = mocker.patch.object(plugin.__class__, "store_output")
        mock_glob.side_effect = [
            ["/tmp/job/output.sim"],  # sim files
            ["/tmp/job/pool_xml_catalog.xml"],  # catalog files
        ]

        result = plugin.post_process(job_path)

        assert result is True
        # Should call store_output for sim and catalog files
        assert mock_store.call_count == 2


class TestLHCbReconstructionPlugin:
    """Test the LHCbReconstructionPlugin class."""

    def test_creation(self):
        """Test LHCbReconstructionPlugin creation."""
        plugin = LHCbReconstructionPlugin(task_id=456, run_id=1)
        assert plugin.name() == "LHCbReconstructionPlugin"
        assert "LHCb reconstruction" in plugin.description

    def test_creation_with_reconstruction_parameters(self):
        """Test creation with reconstruction-specific parameters."""
        plugin = LHCbReconstructionPlugin(
            task_id=456,
            run_id=1,
            input_data_type="SIM",
            output_data_type="DST",
            reconstruction_version="v50r1",
        )
        assert plugin.input_data_type == "SIM"
        assert plugin.output_data_type == "DST"
        assert plugin.reconstruction_version == "v50r1"

    def test_validate_data_types(self):
        """Test data type validation."""
        # Valid data types
        plugin = LHCbReconstructionPlugin(task_id=456, run_id=1, input_data_type="SIM", output_data_type="DST")
        assert plugin.input_data_type == "SIM"
        assert plugin.output_data_type == "DST"

    def test_get_input_query_with_data_type(self):
        """Test input query with data type filtering."""
        plugin = LHCbReconstructionPlugin(task_id=456, run_id=1, input_data_type="SIM")

        result = plugin.get_input_query("input_files")
        expected = Path("filecatalog/lhcb/456/1/sim")
        assert result == expected

    def test_get_output_query_with_data_type(self):
        """Test output query with data type specification."""
        plugin = LHCbReconstructionPlugin(task_id=456, run_id=1, output_data_type="DST")

        result = plugin.get_output_query("output_files")
        expected = Path("filecatalog/lhcb/456/1/dst")
        assert result == expected

    def test_pre_process(self):
        """Test reconstruction pre_process method."""
        plugin = LHCbReconstructionPlugin(task_id=456, run_id=1, reconstruction_version="v50r1")

        command = ["lhcb-reconstruction", "--input", "sim.dst"]
        result = plugin.pre_process({}, None, Path("/tmp/job"), command)

        # Should add reconstruction-specific parameters
        assert "--version" in result
        assert "v50r1" in result
        assert "--input-type" in result
        assert "RAW" in result  # default input_data_type
        assert "--output-type" in result
        assert "DST" in result  # default output_data_type


class TestLHCbAnalysisPlugin:
    """Test the LHCbAnalysisPlugin class."""

    def test_creation(self):
        """Test LHCbAnalysisPlugin creation."""
        plugin = LHCbAnalysisPlugin(task_id=789, run_id=1, analysis_name="TestAnalysis", user_name="testuser")
        assert plugin.name() == "LHCbAnalysisPlugin"
        assert "LHCb analysis" in plugin.description

    def test_creation_with_analysis_parameters(self):
        """Test creation with analysis-specific parameters."""
        plugin = LHCbAnalysisPlugin(
            task_id=789,
            run_id=1,
            analysis_name="B2KstarMuMu",
            user_name="alice",
            analysis_version="v1.0",
        )
        assert plugin.analysis_name == "B2KstarMuMu"
        assert plugin.user_name == "alice"
        assert plugin.analysis_version == "v1.0"

    def test_user_path_generation(self):
        """Test user-specific path generation."""
        plugin = LHCbAnalysisPlugin(task_id=789, run_id=1, user_name="alice", analysis_name="B2KstarMuMu")

        result = plugin.get_input_query("input_data")
        expected = Path("filecatalog/lhcb/analysis/alice/B2KstarMuMu/input")
        assert result == expected

    def test_get_output_query_with_analysis(self):
        """Test output query with analysis-specific paths."""
        plugin = LHCbAnalysisPlugin(task_id=789, run_id=1, user_name="alice", analysis_name="B2KstarMuMu")

        result = plugin.get_output_query("results")
        expected = Path("filecatalog/lhcb/analysis/alice/B2KstarMuMu/results")
        assert result == expected

    def test_pre_process(self):
        """Test analysis pre_process method."""
        plugin = LHCbAnalysisPlugin(task_id=789, run_id=1, analysis_name="B2KstarMuMu", user_name="alice")

        command = ["python", "analysis.py"]
        result = plugin.pre_process({}, None, Path("/tmp/job"), command)

        # Should add analysis-specific parameters
        assert "--analysis" in result
        assert "B2KstarMuMu" in result
        assert "--user" in result
        assert "alice" in result

    def test_post_process_with_user_output(self, mocker):
        """Test analysis post_process with user-specific output handling."""
        plugin = LHCbAnalysisPlugin(task_id=789, run_id=1, user_name="alice", analysis_name="TestAnalysis")

        job_path = Path("/tmp/job")

        # Mock glob to find ROOT files and plot files
        mock_glob = mocker.patch("dirac_cwl.execution_hooks.plugins.lhcb.glob.glob")
        # Mock the store_output method
        mock_store = mocker.patch.object(plugin.__class__, "store_output")
        mock_glob.side_effect = [
            ["/tmp/job/results.root"],  # ROOT files
            [],  # PNG files
            [],  # PDF files
            [],  # EPS files
            [],  # SVG files
        ]

        result = plugin.post_process(job_path)

        # The method should succeed
        assert result is True
        # Should call store_output for ROOT files
        mock_store.assert_called()


class TestLHCbBasePluginIntegration:
    """Test integration between LHCb plugins."""

    def test_all_lhcb_plugins_have_vo_namespace(self):
        """Test that all LHCb plugins have the correct VO namespace."""
        plugins = [
            LHCbBasePlugin,
            LHCbSimulationPlugin,
            LHCbReconstructionPlugin,
            LHCbAnalysisPlugin,
        ]

        for plugin_class in plugins:
            assert hasattr(plugin_class, "vo")
            assert plugin_class.vo == "lhcb"

    def test_lhcb_plugins_inheritance(self):
        """Test that all LHCb plugins inherit from LHCbBasePlugin."""
        plugins = [
            LHCbSimulationPlugin,
            LHCbReconstructionPlugin,
            LHCbAnalysisPlugin,
        ]

        for plugin_class in plugins:
            assert issubclass(plugin_class, LHCbBasePlugin)

    def test_lhcb_path_consistency(self):
        """Test that all LHCb plugins generate consistent paths."""
        plugins = [
            ("simulation", LHCbSimulationPlugin(task_id=1, run_id=1)),
            ("reconstruction", LHCbReconstructionPlugin(task_id=2, run_id=2)),
            (
                "analysis",
                LHCbAnalysisPlugin(task_id=3, run_id=3, analysis_name="test", user_name="test"),
            ),
        ]

        for _plugin_type, plugin in plugins:
            input_path = plugin.get_input_query("test_input")
            output_path = plugin.get_output_query("test_output")

            # All paths should exist (not None for these plugins)
            assert input_path is not None
            assert output_path is not None

            # All paths should contain "filecatalog/lhcb"
            assert str(input_path).startswith("filecatalog/lhcb")
            assert str(output_path).startswith("filecatalog/lhcb")
