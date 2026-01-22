"""
Tests for job submission clients.

This module tests the job submission clients.
"""

from pathlib import Path

import pytest
from cwl_utils.pack import pack
from cwl_utils.parser import load_document
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile

from dirac_cwl_proto.job.submission_clients import DIRACSubmissionClient, PrototypeSubmissionClient
from dirac_cwl_proto.submission_models import (
    JobInputModel,
    JobModel,
)


class TestDIRACSubmissionClient:
    """Test the DIRACSubmissionClient class."""

    @pytest.mark.parametrize(
        "cwl_file, cwl_input, expected_jdl",
        [
            # --- Hello World example ---
            # There is no input expected
            (
                "test/workflows/helloworld/description_basic.cwl",
                None,
                """Executable = dirac-cwl-exec;
Arguments = job.json;
JobName = test;
OutputSandbox = {std.out, std.err};
Priority = 10;
InputSandbox = SB:SandboxSE|/S3/diracx-sandbox-store/sha256:0001.tar.zst;""",
            ),
            # --- Test metadata example ---
            # A string input is passed
            (
                "test/workflows/test_meta/test_meta.cwl",
                None,
                """Executable = dirac-cwl-exec;
Arguments = job.json;
JobName = test;
OutputSandbox = {std.out, std.err};
Priority = 100;
Site = CTAO.DESY-ZN.de, CTAO.PIC.es;
InputSandbox = SB:SandboxSE|/S3/diracx-sandbox-store/sha256:0001.tar.zst;""",
            ),
            # Gather only
            (
                "test/workflows/pi/pigather.cwl",
                "test/workflows/pi/type_dependencies/job/inputs-pi_gather_catalog.yaml",
                """Executable = dirac-cwl-exec;
Arguments = job.json;
NumberOfProcessors = 1;
JobName = test;
OutputSandbox = {std.out, std.err};
Priority = 10;
InputSandbox = SB:SandboxSE|/S3/diracx-sandbox-store/sha256:0001.tar.zst;
InputData = LFN:/pi/100/result_1.sim, LFN:/pi/100/result_2.sim, LFN:/pi/100/result_3.sim, LFN:/pi/100/result_4.sim, \
LFN:/pi/100/result_5.sim;""",
            ),
        ],
    )
    def test_convert_to_jdl(self, cwl_file, cwl_input, expected_jdl):
        """Test convert_to_jdl."""
        submission_client = DIRACSubmissionClient()

        task_path = cwl_file
        task = load_document(pack(task_path))
        sandbox_id = "SB:SandboxSE|/S3/diracx-sandbox-store/sha256:0001.tar.zst"

        task_input = None
        if cwl_input:
            parameter = load_inputfile(cwl_input)
            task_input = JobInputModel(
                sandbox=[sandbox_id],
                cwl=parameter,
            )

        job = JobModel(
            task=task,
            input=task_input,
        )

        res = submission_client.convert_to_jdl(job, sandbox_id)

        assert res == expected_jdl


class TestPrototypeSubmissionClient:
    """Test the PrototypeSubmissionClient class."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("file_paths", [[Path("test/workflows/helloworld/")], [Path("test/workflows/pi/")]])
    async def test_upload_sandbox(self, file_paths):
        """Test upload sandbox."""
        submission_client = PrototypeSubmissionClient()

        sandbox_pfn = await submission_client.upload_sandbox(file_paths)
        assert Path(f"sandboxstore/{sandbox_pfn}").exists()
