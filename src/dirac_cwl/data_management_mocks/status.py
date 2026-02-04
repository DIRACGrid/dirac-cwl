"""Mock for status reports."""

from datetime import datetime, timezone

from diracx.client._generated.models import (
    JobStatus,
    SetJobStatusReturn,
)


async def set_job_status(
    job_id: str,
    status: JobStatus | None = None,
    minor_status: str | None = None,
    application_status: str | None = None,
    source: str = "Unknown",
    timestamp: str | None = None,
    force: bool = False,
) -> SetJobStatusReturn:
    """Set the status of a job.

    :param job_id: Target Job ID
    :type job_id: str
    :param status: Status to set for the job. No change if None.
    :type status: JobStatus | None
    :param minor_status: Minor Status to set for the job. No change if None.
    :type minor_status: str | None
    :param application_status: Application Status to set for the job. No change if None.
    :type application_status: str | None
    :param source: Source of the status (i.e. JobWrapper)
    :type source: str
    :param timestamp: When the status changed. Default is now
    :type timestamp: str | None
    :param force: Whether to force the update. Default is False.
    :type force: bool

    :return: Result of the job status update.
    :rtype: SetJobStatusReturn
    """
    file_path = f"status_{job_id}"
    if not timestamp:
        timestamp = datetime.now(timezone.utc).isoformat()

    with open(file_path, "a+") as f:
        f.seek(0)
        lines = f.readlines()
        if lines:
            (_, _, laststatus, last_minor, last_app) = lines[-1].strip().split(" | ")
            if not status:
                status = JobStatus(laststatus)
            if not minor_status:
                minor_status = last_minor
            if not application_status:
                application_status = last_app
        strStatus = status.value if isinstance(status, JobStatus) else status
        f.write(f"{timestamp} | {source} | {strStatus} | {minor_status} | {application_status}\n")

    return SetJobStatusReturn(success={}, failed={})
