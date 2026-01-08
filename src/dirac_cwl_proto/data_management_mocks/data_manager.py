"""Mock DIRAC data manager for local file storage operations."""

from pathlib import Path

from DIRAC.DataManagementSystem.Client.DataManager import DataManager  # type: ignore[import-untyped]
from DIRAC.Resources.Storage.FileStorage import FileStorage  # type: ignore[import-untyped]
from DIRACCommon.Core.Utilities.ReturnValues import S_ERROR, S_OK, returnSingleResult  # type: ignore[import-untyped]

from dirac_cwl_proto.data_management_mocks.file_catalog import LocalFileCatalog


class MockDataManager(DataManager):
    """Mock DIRAC DataManager for local file storage."""

    def __init__(self):
        """Initialize the mock data manager with local file catalog."""
        self.base_storage_path = "filecatalog"
        self.storage_element = FileStorage("local", {"Path": self.base_storage_path})
        self.fileCatalog = LocalFileCatalog()

    def getFile(self, lfn, destinationDir=".", sourceSE=None, diskOnly=False):
        """Get local copy of LFN(s) from Storage Elements.

        :param mixed lfn: a single LFN or list of LFNs.
        :param str destinationDir: directory to which the file(s) will be
        downloaded. (Default: current working directory).
        :param str sourceSE: source SE from which to download (Default: all replicas will be attempted).
        :param bool diskOnly: chooses the disk ONLY replica(s). (Default: False)
        :return: S_OK({"Successful": {}, "Failed": {}})/S_ERROR(errMessage).
        """
        if isinstance(lfn, list):
            lfns = lfn
        elif isinstance(lfn, str):
            lfns = [lfn]
        else:
            return S_ERROR(f"wrong type for lfn: {lfn}, expected str or list[str]")

        if not sourceSE:
            sourceSE = self.storage_element

        success = {}
        fail = {}
        for lfn in lfns:
            res = sourceSE.getFile(
                str(
                    Path(self.base_storage_path) / str(lfn).removeprefix("lfn:").removeprefix("LFN:").removeprefix("/")
                ),
                destinationDir,
            )
            if not res["OK"]:
                fail[lfn] = res["Message"]
            elif res["Value"]["Failed"]:
                fail[lfn] = res["Value"]["Failed"]
            else:
                success[lfn] = str(Path(destinationDir).resolve() / Path(lfn).name)
        return S_OK({"Successful": success, "Failed": fail})

    def putAndRegister(
        self,
        lfn,
        fileName,
        diracSE,
        guid=None,
        path=None,
        checksum=None,
        overwrite=None,
    ):
        """Put a local file to a Storage Element and register in the File Catalogues.

        :param lfn: The file LFN.
        :param fileName: The full path to the local file.
        :param diracSE: The Storage Element to which to put the file.
        :param guid: The guid with which the file is to be registered (if not provided will be generated).
        :param path: The path on the storage where the file will be put (if not provided the LFN will be used).
        :param checksum: File checksum (optional).
        :param overwrite: Removes file from the file catalogue and SE before attempting upload.
        """
        self.fileCatalog.addFile(lfn)
        return self.put(lfn, fileName, diracSE, path)

    def put(self, lfn, fileName, diracSE, path=None):
        """Put a local file to a Storage Element.

        :param str lfn: LFN.
        :param str fileName: The full path to the local file.
        :param str diracSE: The Storage Element to which to put the file.
        :param str path: The path on the storage where the file will be put (if not provided the LFN will be used).
        :return: S_OK({"Successful": {...}, "Failed": {...}})/S_ERROR(errMessage).
        """
        se = self.storage_element
        if not se:
            return S_ERROR("No Storage Element defined")
        if not path:
            path = str(lfn).removeprefix("lfn:").removeprefix("LFN:").removeprefix("/")
        dest = str(Path(self.base_storage_path) / Path(path))
        res = returnSingleResult(se.putFile({dest: fileName}))
        if not res["OK"]:
            return S_OK({"Successful": {}, "Failed": {lfn: res["Message"]}})
        return S_OK({"Successful": {lfn: res["Value"]}, "Failed": {}})
