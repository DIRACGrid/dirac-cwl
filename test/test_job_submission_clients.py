"""
Tests for job submission clients.

This module tests the job submission clients.
"""

from pathlib import Path

import pytest
from cwl_utils.pack import pack
from cwl_utils.parser import load_document
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile

from dirac_cwl.job.submission_clients import DIRACSubmissionClient, PrototypeSubmissionClient
from dirac_cwl.submission_models import (
    JobInputModel,
    JobModel,
)


class TestDIRACSubmissionClient:
    """Test the DIRACSubmissionClient class."""

    def test_convert_to_jdl_deprecated(self):
        """Test that convert_to_jdl raises NotImplementedError (moved to diracx-logic)."""
        submission_client = DIRACSubmissionClient()
        with pytest.raises(NotImplementedError, match="cwl_to_jdl"):
            submission_client.convert_to_jdl(None, "")


class TestPrototypeSubmissionClient:
    """Test the PrototypeSubmissionClient class."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("file_paths", [[Path("test/workflows/helloworld/")], [Path("test/workflows/pi/")]])
    async def test_create_sandbox(self, file_paths):
        """Test upload sandbox."""
        submission_client = PrototypeSubmissionClient()

        sandbox_pfn = await submission_client.create_sandbox(file_paths)
        assert Path(f"sandboxstore/{sandbox_pfn}").exists()
