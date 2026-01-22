"""Mock DIRAC sandbox store client for local file operations."""

import hashlib
import logging
import os
import random
import tarfile
import tempfile
from pathlib import Path
from typing import Literal, Optional, Sequence

import zstandard
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient  # type: ignore[import-untyped]
from DIRACCommon.Core.Utilities.ReturnValues import S_OK  # type: ignore[import-untyped]
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class MockSandboxInfo(BaseModel):
    """Mock Dirac Sandbox Info."""

    checksum_algorithm: str
    checksum: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    size: int = Field(ge=1)
    format: str


class MockDiracXSandboxAPI:
    """Mock DiracX Sandbox API."""

    def __init__(self):
        """Initialize the mock DiracX Sandbox API."""
        self.sandboxstore_path = Path("sandboxstore")
        self.SANDBOX_CHECKSUM_ALGORITHM = "sha256"
        self.SANDBOX_COMPRESSION: Literal["zst"] = "zst"

    def upload_sandbox(self, paths: list[Path]):
        """Upload a sandbox archive to the sandboxstore.

        :param paths: File paths to be uploaded in the sandbox.
        """
        with tempfile.TemporaryFile(mode="w+b") as tar_fh:
            # Create zstd compressed tar with level 18 and long matching enabled
            compression_params = zstandard.ZstdCompressionParameters.from_level(18, enable_ldm=1)
            cctx = zstandard.ZstdCompressor(compression_params=compression_params)
            with cctx.stream_writer(tar_fh, closefd=False) as compressor:
                with tarfile.open(fileobj=compressor, mode="w|") as tf:
                    for path in paths:
                        logger.debug("Adding %s to sandbox as %s", path.resolve(), path.name)
                        tf.add(path.resolve(), path.name, recursive=True)
            tar_fh.seek(0)

            # Generate sandbox checksum
            hasher = getattr(hashlib, self.SANDBOX_CHECKSUM_ALGORITHM)()
            while data := tar_fh.read(512 * 1024):
                hasher.update(data)
            checksum = hasher.hexdigest()
            tar_fh.seek(0)
            logger.debug("Sandbox checksum is %s", checksum)

            # Store sandbox info
            sandbox_info = MockSandboxInfo(
                checksum_algorithm=self.SANDBOX_CHECKSUM_ALGORITHM,
                checksum=checksum,
                size=os.stat(tar_fh.fileno()).st_size,
                format=f"tar.{self.SANDBOX_COMPRESSION}",
            )

            # Create PFN
            pfn = f"{sandbox_info.checksum_algorithm}:{sandbox_info.checksum}.{sandbox_info.format}"

            # Create sandbox in sandboxstore
            sandbox_path = Path(self.sandboxstore_path / pfn)
            if not sandbox_path.exists():
                with tarfile.open(sandbox_path, "w:gz") as tar:
                    for file in paths:
                        if not file:
                            break
                        if isinstance(file, str):
                            file = Path(file)
                        tar.add(file, arcname=file.name)
                logger.debug("Sandbox uploaded for %s", pfn)
            else:
                logger.debug("Sandbox already exists for %s", pfn)
            return pfn

    def download_sandbox(self, pfn: str, destination: Path):
        """Retrieve a sandbox from the sandboxstore and extract it to the given destination.

        :param pfn: Sandbox PFN
        :param destination: Destination directory
        """
        logger.debug("Retrieving sandbox for %s", pfn)
        sandbox_archive = Path(self.sandboxstore_path / f"{pfn}")
        with tarfile.open(sandbox_archive) as tf:
            tf.extractall(path=Path(destination), filter="data")
        logger.debug("Extracted %s to %s", pfn, destination)


class MockSandboxStoreClient(SandboxStoreClient):
    """Local mock for Dirac's SandboxStore Client."""

    def __init__(self):
        """Initialize the mock sandbox store client."""
        pass

    def uploadFilesAsSandbox(
        self,
        fileList: Sequence[Path | str],
        sizeLimit: int = 0,
        assignTo: Optional[dict] = None,
    ):
        """Create and upload a sandbox archive from a list of files.

        Packages the provided files into a compressed tar archive and stores
        it under the local sandbox directory.

        :param Sequence[Path | str] fileList: Files to be included in the sandbox.
        :param int sizeLimit: Maximum allowed archive size in bytes. Currently unused.
        :param Optional[dict] assignTo: Mapping of job identifiers to sandbox types (e.g. { 'Job:<id>': '<type>' }).

        :return S_OK(sandbox_path): Path to the created sandbox archive, or `None` if no files were provided.
        """
        if len(fileList) == 0:
            return S_OK()
        sandbox_id = random.randint(1000, 9999)
        sandbox_path = Path("sandboxstore") / f"sandbox_{str(sandbox_id)}.tar.gz"
        sandbox_path.parent.mkdir(exist_ok=True, parents=True)
        with tarfile.open(sandbox_path, "w:gz") as tar:
            for file in fileList:
                if not file:
                    break
                if isinstance(file, str):
                    file = Path(file)
                tar.add(file, arcname=file.name)
        res = S_OK(str(sandbox_path))
        res["SandboxFileName"] = f"sandbox_{str(sandbox_id)}.tar.gz"
        return res

    def downloadSandbox(
        self,
        sbLocation: str | Path,
        destinationDir: str = "",
        inMemory: bool = False,
        unpack: bool = True,
    ) -> list[Path]:
        """Download and extract files from a sandbox archive.

        Opens the given sandbox archive and extracts its contents to the specified
        directory.

        :param str|Path sbLocation: Path to the sandbox archive file.
        :param str destinationDir: Directory to extract the files into. Defaults to the current directory.
        :param bool inMemory: Placeholder for in-memory extraction.
        :param bool unpack: Whether to unpack the archive. Only unpacking is currently supported.

        :return S_OK({list[Path]}): List of paths to the extracted files.
        """
        if not unpack or inMemory:
            raise NotImplementedError
        else:
            sandbox_path = Path("sandboxstore") / f"{sbLocation}.tar.gz"
            with tarfile.open(sandbox_path, "r:gz") as tar:
                tar.extractall(destinationDir, filter="data")
                files = tar.getnames()
            logger.info("Files downloaded successfully!")
            return S_OK([str(Path(destinationDir) / file) for file in files])

    def downloadSandboxForJob(self, jobId, sbType, destinationPath="", inMemory=False, unpack=True) -> None:
        """Download sandbox contents for a specific job.

        Placeholder for future implementation of job-based sandbox retrieval.

        :param jobId: Job identifier.
        :param sbType: Sandbox type.
        :param destinationPath: Destination directory path.
        :param inMemory: Whether to load sandbox in memory.
        :param unpack: Whether to unpack the sandbox.
        :raises NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError
