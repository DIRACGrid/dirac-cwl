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
            f.write(
                " | ".join(
                    (
                        "Timestamp".ljust(32),
                        "Source".ljust(10),
                        "Status".ljust(20),
                        "Minor status".ljust(35),
                        "Application status",
                    )
                )
                + "\n"
            )
            f.write(" | ".join(("-" * 32, "-" * 10, "-" * 20, "-" * 35, "-" * 18)) + "\n")
            for timestamp, info in self.job_status_info.items():
                status = info["Status"] or ""
                minor_status = info["MinorStatus"] or ""
                application_status = info["ApplicationStatus"] or ""
                source = info["Source"] or ""
                f.write(
                    " | ".join(
                        (
                            timestamp.ljust(32),
                            source.ljust(10),
                            status.ljust(20),
                            minor_status.ljust(35),
                            application_status,
                        )
                    )
                    + "\n"
                )

    async def sendStoredJobParameters(self):
        """Mock sendStoredJobParameters."""
        STATUS_DIR.mkdir(exist_ok=True)
        file_path = STATUS_DIR / f"job_params_{self.job_id}"
        with open(file_path, "w+") as f:
            f.write(" | ".join(("Name".ljust(20), "Value")) + "\n")
            f.write(" | ".join(("-" * 20, "-" * 20)) + "\n")
            for name, val in self.job_parameters.items():
                f.write(" | ".join((name.ljust(20), val)) + "\n")
