"""Integration tests for CWL workflow execution."""

import os
import platform
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dirac_cwl import app
from dirac_cwl.mocks.sandbox import SANDBOX_STORE_DIR, download_sandbox


def strip_ansi_codes(text: str) -> str:
    """Remove ANSI color codes from text."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


@pytest.fixture()
def cli_runner():
    """Provide a Typer CLI runner for testing."""
    return CliRunner()


@pytest.fixture()
def cleanup():
    """Provide a cleanup function to remove test directories."""

    def _cleanup():
        shutil.rmtree("filecatalog", ignore_errors=True)
        shutil.rmtree("sandboxstore", ignore_errors=True)
        shutil.rmtree("workernode", ignore_errors=True)
        shutil.rmtree("status", ignore_errors=True)

    _cleanup()
    yield
    _cleanup()


@pytest.fixture()
def pi_test_files():
    """Create test files needed for pi workflow tests."""
    # Create job input files
    job_dir = Path("test/workflows/pi/type_dependencies/job")
    job_dir.mkdir(parents=True, exist_ok=True)

    # Create result files for pi-gather job test (result_1.sim through result_5.sim)
    result_files = []
    for i in range(1, 6):
        result_file = job_dir / f"result_{i}.sim"
        with open(result_file, "w") as f:
            # Create different sample data for each file
            f.write(f"0.{i} 0.{i + 1}\n-0.{i + 2} 0.{i + 3}\n0.{i + 4} -0.{i + 5}\n")
        result_files.append(result_file)

    yield

    # Cleanup - remove created files
    for result_file in result_files:
        if result_file.exists():
            result_file.unlink()


# -----------------------------------------------------------------------------
# Job tests
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cwl_file, inputs",
    [
        # --- Hello World example ---
        # There is no input expected
        ("test/workflows/helloworld/description_basic.cwl", []),
        # An input is expected but not required (default value is used)
        ("test/workflows/helloworld/description_with_inputs.cwl", []),
        # A string input is passed
        (
            "test/workflows/helloworld/description_with_inputs.cwl",
            ["test/workflows/helloworld/type_dependencies/job/inputs-helloworld_with_inputs1.yaml"],
        ),
        # Multiple string inputs are passed
        (
            "test/workflows/helloworld/description_with_inputs.cwl",
            [
                "test/workflows/helloworld/type_dependencies/job/inputs-helloworld_with_inputs1.yaml",
                "test/workflows/helloworld/type_dependencies/job/inputs-helloworld_with_inputs2.yaml",
                "test/workflows/helloworld/type_dependencies/job/inputs-helloworld_with_inputs3.yaml",
            ],
        ),
        # --- Test metadata example ---
        # A string input is passed
        ("test/workflows/test_meta/test_meta.cwl", []),
        # --- Test outputs hints example ---
        # Output to filecatalog
        ("test/workflows/test_outputs/test_outputs.cwl", []),
        # Output to sandbox
        ("test/workflows/test_outputs/test_outputs_sandbox.cwl", []),
        # --- Crypto example ---
        # Complete
        (
            "test/workflows/crypto/description.cwl",
            ["test/workflows/crypto/type_dependencies/job/inputs-crypto_complete.yaml"],
        ),
        # Caesar only
        (
            "test/workflows/crypto/caesar.cwl",
            ["test/workflows/crypto/type_dependencies/job/inputs-crypto_complete.yaml"],
        ),
        # ROT13 only
        (
            "test/workflows/crypto/rot13.cwl",
            ["test/workflows/crypto/type_dependencies/job/inputs-crypto_complete.yaml"],
        ),
        # Base64 only
        (
            "test/workflows/crypto/base64.cwl",
            ["test/workflows/crypto/type_dependencies/job/inputs-crypto_complete.yaml"],
        ),
        # MD5 only
        (
            "test/workflows/crypto/md5.cwl",
            ["test/workflows/crypto/type_dependencies/job/inputs-crypto_complete.yaml"],
        ),
        # --- Pi example ---
        # Complete workflow
        (
            "test/workflows/pi/description.cwl",
            ["test/workflows/pi/type_dependencies/job/inputs-pi_complete.yaml"],
        ),
        # Simulate only
        ("test/workflows/pi/pisimulate.cwl", []),
        # Gather only
        (
            "test/workflows/pi/pigather.cwl",
            ["test/workflows/pi/type_dependencies/job/inputs-pi_gather.yaml"],
        ),
    ],
)
def test_run_job_success(cli_runner, cleanup, pi_test_files, cwl_file, inputs):
    """Test successful job submission and execution."""
    # CWL file is the first argument
    command = ["job", "submit", cwl_file]

    # Add the input file(s)
    for input in inputs:
        command.extend(["--parameter-path", input])

    result = cli_runner.invoke(app, command)
    # Remove ANSI color codes for assertion
    clean_output = strip_ansi_codes(result.stdout)
    assert "CLI: Job(s) done" in clean_output, f"Failed to run the job: {result.stdout}"


@pytest.mark.parametrize(
    "cwl_file, inputs, expected_error",
    [
        # The description file is malformed: class attribute is unknown
        (
            "test/workflows/malformed_description/description_malformed_class.cwl",
            [],
            "`class`containsundefinedreferenceto",
        ),
        # The description file is malformed: baseCommand is unknown
        (
            "test/workflows/malformed_description/description_malformed_command.cwl",
            [],
            "invalidfield`baseComand`",
        ),
        # The description file points to a non-existent file (subworkflow)
        (
            "test/workflows/bad_references/reference_doesnotexists.cwl",
            [],
            "Nosuchfileordirectory",
        ),
        # The description file points to another file point to it (circular dependency)
        (
            "test/workflows/bad_references/reference_circular1.cwl",
            [],
            "Recursingintostep",
        ),
        # The description file points to itself (another circular dependency)
        (
            "test/workflows/bad_references/reference_circular1.cwl",
            [],
            "Recursingintostep",
        ),
    ],
)
def test_run_job_validation_failure(cli_runner, cleanup, cwl_file, inputs, expected_error):
    """Test job submission fails with invalid workflow or inputs."""
    command = ["job", "submit", cwl_file]
    for input in inputs:
        command.extend(["--parameter-path", input])
    result = cli_runner.invoke(app, command)
    clean_stdout = strip_ansi_codes(result.stdout)
    assert "Job(s) done" not in clean_stdout, "The job did complete successfully."

    # Check all possible output sources
    clean_output = re.sub(r"\s+", "", result.stdout)
    try:
        clean_stderr = re.sub(r"\s+", "", result.stderr or "")
    except (ValueError, AttributeError):
        clean_stderr = ""
    clean_exception = re.sub(r"\s+", "", str(result.exception) if result.exception else "")

    # Handle different possible error messages for circular references
    if expected_error == "Recursingintostep":
        # Accept multiple possible error patterns for circular references
        circular_ref_patterns = [
            "Recursingintostep",
            "maximumrecursiondepthexceeded",
            "RecursionError",
            "circularreference",
        ]
        error_found = any(
            pattern in clean_output or pattern in clean_stderr or pattern in clean_exception
            for pattern in circular_ref_patterns
        )
        assert error_found, (
            f"None of the expected circular reference error patterns found in "
            f"stdout: {clean_output}, stderr: {clean_stderr}, exception: {clean_exception}"
        )
    else:
        error_found = (
            expected_error in clean_output or expected_error in clean_stderr or expected_error in clean_exception
        )
        assert error_found, (
            f"Expected error '{expected_error}' not found in "
            f"stdout: {clean_output}, stderr: {clean_stderr}, exception: {clean_exception}"
        )


@pytest.mark.skipif(
    platform.system() != "Linux" or not shutil.which("taskset"), reason="taskset command only available on Linux"
)
def test_run_job_parallely(tmp_path, cleanup):
    """Test parallel job execution performance."""
    # Ensure the workflow exists
    workflow = Path("test/workflows/parallel/description.cwl").resolve()
    assert workflow.exists()

    # Ensure we actually have >=2 CPUs available on this runner for the parallel half
    allowed = os.sched_getaffinity(0)
    if len(allowed) < 2:
        pytest.skip("Runner exposes only 1 CPU")

    # Pick two CPUs to use
    allowed = sorted(allowed)
    cpu0, cpu1 = allowed[0], allowed[1]

    def read_interval(p: Path):
        """Read interval."""
        start, end = p.read_text().splitlines()
        return int(start), int(end)

    def overlaps(a, b):
        """Check if two intervals overlap."""
        (a0, a1), (b0, b1) = a, b
        return a0 < b1 and b0 < a1

    def run(cpu_spec: str):
        subprocess.run(
            ["taskset", "-c", cpu_spec, "dirac-cwl", "job", "submit", str(workflow)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=True,
        )

        # TODO: need to find a way to get a job id and associate the output sandbox to the job id
        sandbox_files = list(Path(SANDBOX_STORE_DIR).glob("*.tar.*"))
        assert len(sandbox_files) == 1, f"Expected exactly one sandbox file, found {len(sandbox_files)}"
        sandbox_file = sandbox_files[0].name
        download_sandbox(str(sandbox_file), tmp_path)

        # Get the output sandbox paths
        a = read_interval(tmp_path / "a.txt")
        b = read_interval(tmp_path / "b.txt")

        # TODO: no need that line once we assign sandbox to job id
        (Path(SANDBOX_STORE_DIR) / sandbox_file).unlink()  # Clean up sandboxstore for next run
        return a, b

    # 1 CPU => expect no overlap
    a1, b1 = run(str(cpu0))
    assert not overlaps(a1, b1), f"Expected sequential execution on 1 CPU, got overlap: a={a1}, b={b1}"

    # 2 CPUs => expect overlap
    a2, b2 = run(f"{cpu0},{cpu1}")
    assert overlaps(a2, b2), f"Expected parallel execution on 2 CPUs, got no overlap: a={a2}, b={b2}"


@pytest.mark.parametrize(
    "cwl_file, inputs, destination_source_input_data",
    [
        # --- Pi example ---
        (
            "test/workflows/pi/pigather.cwl",
            ["test/workflows/pi/type_dependencies/job/inputs-pi_gather_catalog.yaml"],
            {
                "filecatalog/pi/100": [
                    "test/workflows/pi/type_dependencies/job/result_1.sim",
                    "test/workflows/pi/type_dependencies/job/result_2.sim",
                    "test/workflows/pi/type_dependencies/job/result_3.sim",
                    "test/workflows/pi/type_dependencies/job/result_4.sim",
                    "test/workflows/pi/type_dependencies/job/result_5.sim",
                ]
            },
        ),
    ],
)
def test_run_job_with_input_data(cli_runner, cleanup, pi_test_files, cwl_file, inputs, destination_source_input_data):
    """Test job execution with input data from file catalog."""
    for destination, inputs_data in destination_source_input_data.items():
        # Copy the input data to the destination
        destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        for input in inputs_data:
            shutil.copy(input, destination)

    # CWL file is the first argument
    command = ["job", "submit", cwl_file]

    # Add the input file(s)
    for input in inputs:
        command.extend(["--parameter-path", input])

    result = cli_runner.invoke(app, command)
    # Remove ANSI color codes for assertion
    clean_output = strip_ansi_codes(result.stdout)
    assert "CLI: Job(s) done" in clean_output, f"Failed to run the job: {result.stdout}"


# -----------------------------------------------------------------------------
# Transformation tests
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cwl_file",
    [
        # --- Hello World example ---
        # There is no input expected
        "test/workflows/helloworld/description_basic.cwl",
        # --- Crypto example ---
        # Complete
        "test/workflows/crypto/description.cwl",
        # Caesar only
        "test/workflows/crypto/caesar.cwl",
        # ROT13 only
        "test/workflows/crypto/rot13.cwl",
        # Base64 only
        "test/workflows/crypto/base64.cwl",
        # MD5 only
        "test/workflows/crypto/md5.cwl",
        # --- Pi example ---
        # Pi simulate transformation
        "test/workflows/pi/pisimulate.cwl",
    ],
)
def test_run_nonblocking_transformation_success(cli_runner, cleanup, cwl_file):
    """Test successful non-blocking transformation submission and execution."""
    # CWL file is the first argument
    command = ["transformation", "submit", cwl_file]

    result = cli_runner.invoke(app, command)
    clean_output = strip_ansi_codes(result.stdout)
    assert "Transformation done" in clean_output, f"Failed to run the transformation: {result.stdout}"


@pytest.mark.parametrize(
    "cwl_file, destination_source_input_data",
    [
        # --- Pi example ---
        # Pi gather transformation (waits for simulation result files)
        (
            "test/workflows/pi/pigather.cwl",
            {
                "filecatalog/pi/100/input-data": [
                    ("result_1.sim", "0.1 0.2\n-0.3 0.4\n0.5 -0.6\n"),
                    ("result_2.sim", "-0.1 0.8\n0.9 -0.2\n-0.7 0.3\n"),
                    ("result_3.sim", "0.4 0.5\n-0.8 -0.1\n0.6 0.7\n"),
                    ("result_4.sim", "-0.9 0.0\n0.2 -0.4\n-0.5 0.8\n"),
                    ("result_5.sim", "0.3 -0.7\n-0.6 0.1\n0.9 -0.2\n"),
                ]
            },
        ),
    ],
)
def test_run_blocking_transformation_success(cli_runner, cleanup, cwl_file, destination_source_input_data):
    """Test successful blocking transformation with input data dependencies."""

    # Define a function to run the transformation command and return the result
    def run_transformation():
        command = ["transformation", "submit", cwl_file]
        return cli_runner.invoke(app, command)

    # Start running the transformation in a separate thread and capture the result
    transformation_result = None

    def run_and_capture():
        nonlocal transformation_result
        transformation_result = run_transformation()

    # Start the transformation in a separate thread (it will wait for files)
    transformation_thread = threading.Thread(target=run_and_capture)
    transformation_thread.start()

    # Give it some time to start and begin waiting for input files
    time.sleep(2)

    # Verify the transformation is still running (waiting for files)
    assert transformation_thread.is_alive(), "The transformation should be waiting for files."

    # Now create the input files (simulating files becoming available)
    for destination, inputs in destination_source_input_data.items():
        # Create the destination directory and files with content
        destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        for filename, content in inputs:
            file_path = destination / filename
            with open(file_path, "w") as f:
                f.write(content)

    # Wait for the transformation to detect the files and complete
    transformation_thread.join(timeout=30)

    # Check if the transformation completed successfully
    assert transformation_result is not None, "The transformation result was not captured."
    clean_transformation_output = strip_ansi_codes(transformation_result.stdout)
    assert "Transformation done" in clean_transformation_output, "The transformation did not complete successfully."


@pytest.mark.parametrize(
    "cwl_file, expected_error",
    [
        # The description file is malformed: class attribute is unknown
        (
            "test/workflows/malformed_description/description_malformed_class.cwl",
            "`class`containsundefinedreferenceto",
        ),
        # The description file is malformed: baseCommand is unknown
        (
            "test/workflows/malformed_description/description_malformed_command.cwl",
            "invalidfield`baseComand`",
        ),
        # The description file points to a non-existent file (subworkflow)
        (
            "test/workflows/bad_references/reference_doesnotexists.cwl",
            "Nosuchfileordirectory",
        ),
        # The description file points to another file point to it (circular dependency)
        (
            "test/workflows/bad_references/reference_circular1.cwl",
            "Recursingintostep",
        ),
        # The description file points to itself (another circular dependency)
        (
            "test/workflows/bad_references/reference_circular1.cwl",
            "Recursingintostep",
        ),
    ],
)
def test_run_transformation_validation_failure(cli_runner, cwl_file, cleanup, expected_error):
    """Test transformation submission fails with invalid workflow."""
    command = ["transformation", "submit", cwl_file]
    result = cli_runner.invoke(app, command)
    clean_stdout = strip_ansi_codes(result.stdout)
    assert "Transformation done" not in clean_stdout, "The transformation did complete successfully."

    # Check all possible output sources
    clean_output = re.sub(r"\s+", "", result.stdout)
    try:
        clean_stderr = re.sub(r"\s+", "", result.stderr or "")
    except (ValueError, AttributeError):
        clean_stderr = ""
    clean_exception = re.sub(r"\s+", "", str(result.exception) if result.exception else "")

    # Handle multiple possible error patterns for circular references
    if expected_error == "Recursingintostep":
        # Check for various circular reference error patterns
        circular_ref_patterns = [
            "Recursingintostep",
            "RecursionError",
            "maximumrecursiondepthexceeded",
            "circularreference",
        ]
        error_found = any(
            pattern in clean_output or pattern in clean_stderr or pattern in clean_exception
            for pattern in circular_ref_patterns
        )
        assert error_found, (
            f"None of the expected circular reference error patterns were found in "
            f"stdout: {clean_output}, stderr: {clean_stderr}, exception: {clean_exception}"
        )
    else:
        error_found = (
            expected_error in clean_output or expected_error in clean_stderr or expected_error in clean_exception
        )
        assert error_found, (
            f"Expected error '{expected_error}' not found in "
            f"stdout: {clean_output}, stderr: {clean_stderr}, exception: {clean_exception}"
        )


# -----------------------------------------------------------------------------
# Production tests
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cwl_file",
    [
        # --- Crypto example ---
        # Complete workflow with independent steps (ideal for production mode)
        "test/workflows/crypto/description.cwl"
    ],
)
def test_run_simple_production_success(cli_runner, cleanup, pi_test_files, cwl_file):
    """Test successful production workflow submission and execution."""
    # CWL file is the first argument
    command = ["production", "submit", cwl_file]

    result = cli_runner.invoke(app, command)
    clean_output = strip_ansi_codes(result.stdout)
    assert "Production done" in clean_output, f"Failed to run the production: {result.stdout}"


@pytest.mark.parametrize(
    "cwl_file, expected_error",
    [
        # The description file is malformed: class attribute is unknown
        (
            "test/workflows/malformed_description/description_malformed_class.cwl",
            "`class`containsundefinedreferenceto",
        ),
        # The description file is malformed: baseCommand is unknown
        (
            "test/workflows/malformed_description/description_malformed_command.cwl",
            "invalidfield`baseComand`",
        ),
        # The description file points to a non-existent file (subworkflow)
        (
            "test/workflows/bad_references/reference_doesnotexists.cwl",
            "Nosuchfileordirectory",
        ),
        # The description file points to another file point to it (circular dependency)
        (
            "test/workflows/bad_references/reference_circular1.cwl",
            "Recursingintostep",
        ),
        # The description file points to itself (another circular dependency)
        (
            "test/workflows/bad_references/reference_circular1.cwl",
            "Recursingintostep",
        ),
        # The workflow is a CommandLineTool instead of a Workflow
        (
            "test/workflows/helloworld/description_basic.cwl",
            "InputshouldbeaninstanceofWorkflow",
        ),
    ],
)
def test_run_production_validation_failure(cli_runner, cleanup, cwl_file, expected_error):
    """Test production submission fails with invalid workflow."""
    command = ["production", "submit", cwl_file]
    result = cli_runner.invoke(app, command)

    clean_stdout = strip_ansi_codes(result.stdout)
    assert "Transformation done" not in clean_stdout, "The transformation did complete successfully."

    # Check all possible output sources
    clean_output = re.sub(r"\s+", "", f"{result.stdout}")
    try:
        clean_stderr = re.sub(r"\s+", "", result.stderr or "")
    except (ValueError, AttributeError):
        clean_stderr = ""
    clean_exception = re.sub(r"\s+", "", f"{result.exception}")

    if expected_error == "Recursingintostep":
        # Check for various circular reference error patterns
        circular_ref_patterns = [
            "Recursingintostep",
            "RecursionError",
            "maximumrecursiondepthexceeded",
            "circularreference",
        ]
        error_found = any(
            pattern in clean_output or pattern in clean_stderr or pattern in clean_exception
            for pattern in circular_ref_patterns
        )
        assert error_found, (
            f"None of the expected circular reference error patterns were found in "
            f"stdout: {clean_output}, stderr: {clean_stderr}, exception: {clean_exception}"
        )
    else:
        error_found = (
            expected_error in clean_output or expected_error in clean_stderr or expected_error in clean_exception
        )
        assert error_found, (
            f"Expected error '{expected_error}' not found in "
            f"stdout: {clean_output}, stderr: {clean_stderr}, exception: {clean_exception}"
        )
