"""Mock file catalog implementations for local testing."""

import json
import time
from pathlib import Path

from DIRAC import S_ERROR, S_OK  # type: ignore[import-untyped]
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog  # type: ignore[import-untyped]


class InMemoryFileCatalog(FileCatalog):
    """Minimal in-memory FileCatalog compatible with DIRAC DataManager."""

    def __init__(self, catalogs=None, vo=None):
        """Initialize the in-memory file catalog.

        :param catalogs: Catalog configuration (unused).
        :param vo: Virtual organization (unused).
        """
        self._eligibleCatalogs = {}
        self._files = {}  # store metadata and logical file names
        super(FileCatalog, self).__init__()

    def _getEligibleCatalogs(self):
        """Get eligible catalogs for this file catalog.

        :return: S_OK with catalog configuration.
        """
        self._eligibleCatalogs = {"MyMockCatalog": {"Type": "MockFileCatalog", "Backend": "Memory"}}
        return S_OK(self._eligibleCatalogs)

    def findFile(self, lfn):
        """Find a file in the catalog by LFN.

        :param lfn: Logical file name.
        :return: S_OK with file metadata or S_ERROR if not found.
        """
        if lfn in self._files:
            return S_OK([self._files[lfn]])
        return S_ERROR(f"File {lfn} not found")

    def addFile(self, lfn, metadata=None):
        """Add a file to the catalog.

        :param lfn: Logical file name.
        :param metadata: Optional file metadata.
        :return: S_OK with LFN or S_ERROR if file already exists.
        """
        if lfn in self._files:
            return S_ERROR(f"File {lfn} already exists")
        self._files[lfn] = {"LFN": lfn, "Metadata": metadata or {}}
        return S_OK(lfn)


class LocalFileCatalog(FileCatalog):
    """File catalog implementation using local filesystem storage."""

    def __init__(self, catalogs=None, vo=None):
        """Initialize the local file catalog.

        :param catalogs: Catalog configuration (unused).
        :param vo: Virtual organization (unused).
        """
        self._eligibleCatalogs = {"MyMockCatalog": {"Type": "MockFileCatalog", "Backend": "LocalFileSystem"}}
        self._metadataPath = "filecatalog/metadata.json"
        super(FileCatalog, self).__init__()

    def _getEligibleCatalogs(self):
        """Get eligible catalogs for this file catalog.

        :return: S_OK with catalog configuration.
        """
        return S_OK(self._eligibleCatalogs)

    def getFileMetadata(self, lfn):
        """Get metadata for a file.

        :param lfn: Logical file name.
        :return: S_OK with metadata dict or failed dict.
        """
        metaAll = self._getAllMetadata()
        if lfn not in metaAll:
            return S_OK({"Successful": {}, "Failed": {lfn: f"File {lfn} not found"}})
        return S_OK({"Successful": {lfn: metaAll[lfn]}, "Failed": {}})

    def addFile(self, lfn):
        """Add a file to the catalog.

        :param lfn: Logical file name.
        :return: S_OK with success/failed dict or S_ERROR if file exists.
        """
        if lfn in self._getAllMetadata():
            return S_ERROR(f"File {lfn} already exists")
        self.setMetadata(lfn, {"CreationDate": time.time()})
        return S_OK({"Successful": {lfn: True}, "Failed": {}})

    def setMetadata(self, lfn, metadataDict):
        """Set metadata for a file.

        :param lfn: Logical file name.
        :param metadataDict: Metadata dictionary to set.
        :return: S_OK with success/failed dict or S_ERROR on failure.
        """
        meta = self._getAllMetadata()
        meta[lfn] = metadataDict

        try:
            self._setAllMetadata(meta)
        except Exception as e:
            return S_ERROR(f"Could set metadata: {e}")
        return S_OK({"Successful": {lfn: True}, "Failed": {}})

    def _getAllMetadata(self):
        """Get all metadata from the local file.

        :return: Dictionary of all file metadata.
        """
        try:
            with open(self._metadataPath, "r") as file:
                meta = json.load(file)
        except Exception:
            meta = {}
        return meta

    def _setAllMetadata(self, metadata):
        """Save all metadata to the local file.

        :param metadata: Dictionary of file metadata to save.
        """
        Path(self._metadataPath).parent.mkdir(parents=True, exist_ok=True)
        with open(self._metadataPath, "w+") as file:
            json.dump(metadata, file)
