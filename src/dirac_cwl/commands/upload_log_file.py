"""Post-processing command for uploading logging information to a Storage Element."""

import glob
import os
import random
import stat
import time
import zipfile
from urllib.parse import urljoin

from DIRAC import S_ERROR, S_OK, siteName
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.Adler import fileAdler
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
from DIRAC.DataManagementSystem.Utilities.ResolveSE import getDestinationSEList
from DIRAC.Resources.Catalog.PoolXMLFile import getGUID
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport

from dirac_cwl.commands import PostProcessCommand


class UploadLogFile(PostProcessCommand):
    """Post-processing command for log file uploading."""

    def execute(self, job_path, **kwargs):
        """Execute the log uploading process.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        # Obtain workflow information
        job_id = kwargs.get("job_id", None)
        production_id = kwargs.get("production_id", None)
        namespace = kwargs.get("namespace", None)
        config_version = kwargs.get("config_version", None)

        if not job_path or not production_id or not namespace or not config_version:
            return S_ERROR("Not enough information to perform the log upload")

        ops = Operations()
        log_extensions = ops.getValue("LogFiles/Extensions", [])
        log_se = ops.getValue("LogStorage/LogSE", "LogSE")

        job_report = JobReport(job_id)

        output_files = self.obtain_output_files(job_path, log_extensions)

        if not output_files:
            return S_OK("No files to upload")

        # Zip files
        zip_name = job_id.zfill(8) + ".zip"
        zip_path = os.path.join(job_path, zip_name)

        try:
            self.zip_files(zip_path, output_files)
        except (AttributeError, OSError, ValueError) as e:
            job_report.setApplicationStatus("Failed to create zip of log files")
            return S_OK(f"Failed to zip files: {repr(e)}")

        # Obtain the log destination
        zip_lfn = self.get_zip_lfn(production_id, job_id, namespace, config_version)

        # Upload to the SE
        result = returnSingleResult(StorageElement(log_se).putFile({zip_lfn: zip_path}))

        if not result["OK"]:  # Failed to uplaod to the LogSE
            result = self.generate_failover_transfer(zip_path, zip_name, zip_lfn)

            if not result["OK"]:
                job_report.setApplicationStatus("Failed To Upload Logs")
                return S_ERROR("Failed to upload to FailoverSE")

        # Set the Log URL parameter
        result = returnSingleResult(StorageElement(log_se).getURL(zip_path, protocol="https"))
        if not result["OK"]:
            # The rule for interpreting what is to be deflated can be found in /eos/lhcb/grid/prod/lhcb/logSE/.htaccess
            logHttpsURL = urljoin("https://lhcb-dirac-logse.web.cern.ch/lhcb-dirac-logse/", zip_lfn)
        else:
            logHttpsURL = result["Value"]

        logHttpsURL = logHttpsURL.replace(".zip", "/")
        job_report.setJobParameter("Log URL", f'<a href="{logHttpsURL}">Log file directory</a>')

        return S_OK("Log Files uploaded")

    def zip_files(self, outputFile, files=None, directory=None):
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

    def obtain_output_files(self, job_path, extensions=[]):
        """Obtain the files to be added to the log zip from the outputs."""
        log_file_extensions = extensions

        if not log_file_extensions:
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

    def get_zip_lfn(self, production_id, job_id, namespace, config_version):
        """Form a logical file name from certain information from the workflow."""
        production_id = str(production_id).zfill(8)
        job_id = str(job_id).zfill(8)
        jobindex = str(int(int(job_id) / 10000)).zfill(4)

        log_path = os.path.join("/lhcb", namespace, config_version, "LOG", production_id, jobindex, "")
        path = os.path.join(log_path, f"{job_id}.zip")
        return path

    def generate_failover_transfer(self, zip_path, zip_name, zip_lfn):
        """Prepare a failover transfer ."""
        failoverSEs = getDestinationSEList("Tier1-Failover", siteName())
        random.shuffle(failoverSEs)

        fileMetaDict = {
            "Size": os.path.getsize(zip_path),
            "LFN": zip_lfn,
            "GUID": getGUID(zip_path),
            "Checksum": fileAdler(zip_path),
            "ChecksumType": "ADLER32",
        }

        return FailoverTransfer().transferAndRegisterFile(
            fileName=zip_name,
            localPath=zip_path,
            lfn=zip_lfn,
            destinationSEList=failoverSEs,
            fileMetaDict=fileMetaDict,
            masterCatalogOnly=True,
        )
