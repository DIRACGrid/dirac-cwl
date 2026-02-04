"""Post-processing command for uploading logging information to a Storage Element."""

import glob
import os
import stat
import time
import zipfile

from DIRACCommon.Core.Utilities.ReturnValues import S_ERROR, S_OK, returnSingleResult

from dirac_cwl_proto.commands import PostProcessCommand
from dirac_cwl_proto.data_management_mocks.data_manager import MockDataManager as DataManager


def zip_files(outputFile, files=None, directory=None):
    """Zip list of files."""
    with zipfile.ZipFile(outputFile, "w") as zipped:
        for fileIn in files:
            # ZIP does not support timestamps before 1980, so for those we simply "touch"
            st = os.stat(fileIn)
            mtime = time.localtime(st.st_mtime)
            dateTime = mtime[0:6]
            if dateTime[0] < 1980:
                os.utime(fileIn, None)  # same as "touch"

            zipped.write(fileIn)


def obtain_output_files(job_path):
    """Obtain the files to be added to the log zip from the outputs."""
    log_file_extensions = [
        "*.txt",
        "*.log",
        "*.out",
        "*.output",
        "*.xml",
        "*.sh",
        "*.info",
        "*.err",
        "prodConf*.py",
        "prodConf*.json",
    ]

    files = []

    for extension in log_file_extensions:
        glob_list = glob.glob(extension, root_dir=job_path, recursive=True)
        for check in glob_list:
            path = os.path.join(job_path, check)
            if os.path.isfile(path):
                os.chmod(path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH + stat.S_IXOTH)
                files.append(path)

    return files


def get_zip_lfn(production_id, job_id, namespace, config_version):
    """Form a logical file name from certain information from the workflow."""
    production_id = str(production_id).zfill(8)
    job_id = str(job_id).zfill(8)
    jobindex = str(int(int(job_id) / 10000)).zfill(4)

    log_path = os.path.join("/lhcb", namespace, config_version, "LOG", production_id, jobindex, "")
    file_path = os.path.join(log_path, f"{job_id}.zip")
    return file_path


class UploadLogFile(PostProcessCommand):
    """Post-processing command for log file uploading."""

    def execute(self, job_path, **kwargs):
        """Execute the log uploading process.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        # Obtain workflow information
        output_files = kwargs.get("outputs", None)
        job_id = kwargs.get("job_id", None)
        production_id = kwargs.get("production_id", None)
        namespace = kwargs.get("namespace", None)
        config_version = kwargs.get("config_version", None)

        if not output_files:
            output_files = obtain_output_files(job_path)

        if not job_path or not production_id or not namespace or not config_version:
            return S_ERROR("Not enough information to perform the log upload")

        # Zip files
        zip_name = job_id.zfill(8) + ".zip"
        zip_path = os.path.join(job_path, zip_name)
        zip_files(zip_path, output_files)

        # Obtain the log destination
        file_lfn = get_zip_lfn(production_id, job_id, namespace, config_version)

        # Upload to the SE
        dm = DataManager()
        result = returnSingleResult(dm.put(file_lfn, zip_path, "LogSE"))

        if not result["OK"]:  # Failed to uplaod to the LogSE
            # TODO: "Tier1-Failover" should be a list of SEs and try until either it works or runs out of possible SEs
            #   The list is obtained from getDestinationSEList at ResolveSE.py in DIRAC
            #   The retry is done at transferAndRegisterFile at FailoverTransfer.py in DIRAC
            result = returnSingleResult(dm.putAndRegister(file_lfn, zip_path, "Tier1-Failover"))
            if not result["OK"]:  # Failed to upload to the Failover SE
                return S_ERROR("Failed to upload to FailoverSE")

        return S_OK()
