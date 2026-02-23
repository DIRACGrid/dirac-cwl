"""Mock for status reports."""

from pathlib import Path

from dirac_cwl.job.job_report import JobReport

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
STATUS_DIR = PROJECT_ROOT / "status"


class JobReportMock(JobReport):
    """Mock JobReport."""

    async def sendStoredStatusInfo(self):
        """Mock sendStoredStatusInfo."""
        STATUS_DIR.mkdir(exist_ok=True)
        file_path = STATUS_DIR / f"status_{self.job_id}"
        with open(file_path, "w+") as f:
            for status, minor_status, application_status, timestamp in self.job_status_info:
                if not status:
                    status = ""
                if not minor_status:
                    minor_status = ""
                f.write(
                    " | ".join((timestamp, self.source, status.ljust(20), minor_status.ljust(35), application_status))
                    + "\n"
                )

    async def sendStoredJobParameters(self):
        """Mock sendStoredJobParameters."""
        STATUS_DIR.mkdir(exist_ok=True)
        file_path = STATUS_DIR / f"job_params_{self.job_id}"
        with open(file_path, "w+") as f:
            for name, val in self.job_parameters:
                f.write(" | ".join((name.ljust(20), val.ljust(30))) + "\n")
