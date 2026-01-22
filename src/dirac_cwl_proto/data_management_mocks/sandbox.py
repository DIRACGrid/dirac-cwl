"""Mock DIRAC sandbox store client for local file operations."""

import hashlib
import logging
import os
import tarfile
import tempfile
from pathlib import Path
from typing import Literal, Sequence

import zstandard
from diracx.core.models import SandboxInfo

logger = logging.getLogger(__name__)

SANDBOX_CHECKSUM_ALGORITHM = "sha256"
SANDBOX_COMPRESSION: Literal["zst"] = "zst"


def create_sandbox(paths: Sequence[str | Path]):
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
                    if isinstance(path, str):
                        path = Path(path)
                    logger.debug("Adding %s to sandbox as %s", path.resolve(), path.name)
                    tf.add(path.resolve(), path.name, recursive=True)
        tar_fh.seek(0)

        # Generate sandbox checksum
        hasher = getattr(hashlib, SANDBOX_CHECKSUM_ALGORITHM)()
        while data := tar_fh.read(512 * 1024):
            hasher.update(data)
        checksum = hasher.hexdigest()
        tar_fh.seek(0)
        logger.debug("Sandbox checksum is %s", checksum)

        # Store sandbox info
        sandbox_info = SandboxInfo(
            checksum_algorithm=SANDBOX_CHECKSUM_ALGORITHM,
            checksum=checksum,
            size=os.stat(tar_fh.fileno()).st_size,
            format=f"tar.{SANDBOX_COMPRESSION}",
        )

        # Create PFN
        pfn = f"{sandbox_info.checksum_algorithm}:{sandbox_info.checksum}.{sandbox_info.format}"

        # Create sandbox in sandboxstore
        sandbox_path = Path(f"sandboxstore/{pfn}")
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


def download_sandbox(pfn: str, destination: Path):
    """Retrieve a sandbox from the sandboxstore and extract it to the given destination.

    :param pfn: Sandbox PFN
    :param destination: Destination directory
    """
    logger.debug("Retrieving sandbox for %s", pfn)
    sandbox_archive = Path(f"sandboxstore/{pfn}")
    with tarfile.open(sandbox_archive) as tf:
        tf.extractall(path=Path(destination), filter="data")
    logger.debug("Extracted %s to %s", pfn, destination)
