"""All classes related to job reports."""

from datetime import datetime, timezone
from enum import StrEnum

from diracx.client.aio import AsyncDiracClient
from diracx.core.models.job import JobStatus, JobStatusUpdate


class JobMinorStatus(StrEnum):
    """List of all available job minor statuses."""

    APPLICATION = "Executing Payload"
    APP_ERRORS = "Application Finished With Errors"
    # APP_NOT_FOUND = "Application not found"
    APP_SUCCESS = "Application Finished Successfully"
    # APP_THREAD_FAILED = "Application thread failed"
    # APP_THREAD_NOT_COMPLETE = "Application thread did not complete"
    DOWNLOADING_INPUT_SANDBOX = "Downloading InputSandbox"
    # DOWNLOADING_INPUT_SANDBOX_LFN = "Downloading InputSandbox LFN(s)"
    # EXCEPTION_DURING_EXEC = "Exception During Execution"
    EXEC_COMPLETE = "Execution Complete"
    FAILED_DOWNLOADING_INPUT_SANDBOX = "Failed Downloading InputSandbox"
    # FAILED_DOWNLOADING_INPUT_SANDBOX_LFN = "Failed Downloading InputSandbox LFN(s)"
    # FAILED_SENDING_REQUESTS = "Failed sending requests"
    # GOING_RESCHEDULE = "Going to reschedule job"
    # ILLEGAL_JOB_JDL = "Illegal Job JDL"
    INPUT_DATA_RESOLUTION = "Resolving Input Data"
    # INPUT_NOT_AVAILABLE = "Input Data Not Available"
    # JOB_EXCEEDED_CPU = "Job has reached the CPU limit of the queue"
    # JOB_EXCEEDED_WALL_CLOCK = "Job has exceeded maximum wall clock time"
    JOB_INITIALIZATION = "Initializing Job"
    # JOB_INSUFFICIENT_DISK = "Job has insufficient disk space to continue"
    # JOB_WRAPPER_EXECUTION = "JobWrapper execution"
    # JOB_WRAPPER_INITIALIZATION = "Job Wrapper Initialization"
    # MARKED_FOR_TERMINATION = "Marked for termination"
    # NO_CANDIDATE_SITE_FOUND = "No candidate sites available"
    OUTPUT_DATA_UPLOADED = "Output Data Uploaded"
    OUTPUT_SANDBOX_UPLOADED = "Output Sandbox Uploaded"
    # PENDING_REQUESTS = "Pending Requests"
    # PILOT_AGENT_SUBMISSION = "Pilot Agent Submission"
    # RECEIVED_KILL_SIGNAL = "Received Kill signal"
    # REQUESTS_DONE = "Requests done"
    # RESCHEDULED = "Job Rescheduled"
    RESOLVING_OUTPUT_SANDBOX = "Resolving Output Sandbox"
    # STALLED_PILOT_NOT_RUNNING = "Job stalled: pilot not running"
    # UPLOADING_JOB_OUTPUTS = "Uploading Outputs"
    UPLOADING_OUTPUT_DATA = "Uploading Output Data"
    UPLOADING_OUTPUT_SANDBOX = "Uploading Output Sandbox"
    # WATCHDOG_STALLED = "Watchdog identified this job as stalled"


class JobReport:
    """JobReport."""

    def __init__(self, job_id: int, source: str, client: AsyncDiracClient | None) -> None:
        """
        Initialize Job Report.

        :param job_id: the job ID
        :param source: source for the reports
        :param client: DiracX client instance
        """
        self.job_status_info: dict[str, JobStatusUpdate] = {}  # where job status updates are cumulated
        self.job_parameters: dict[str, str] = {}  # where job parameters are cumulated
        self.job_id = job_id
        self.source = source
        self._client = client

    def setJobStatus(
        self,
        status: JobStatus | None = None,
        minor_status: JobMinorStatus | None = None,
        application_status: str | None = None,
    ) -> None:
        """
        Add a new job status to the job report.

        :param status: job status
        :param minorStatus: job minor status
        :param applicationStatus: application status
        """
        timestamp = str(datetime.now(timezone.utc))
        # add job status record
        self.job_status_info.update(
            {
                timestamp: JobStatusUpdate(
                    Status=status,
                    MinorStatus=minor_status,
                    ApplicationStatus=application_status,
                    Source=self.source,
                )
            }
        )

    def setApplicationStatus(self, application_status: str | None = None):
        """
        Accumulate application status.

        :param appStatus: application status to set
        """
        timeStamp = str(datetime.now(timezone.utc))
        if not isinstance(application_status, str):
            application_status = repr(application_status)
        # add Application status record
        if application_status:
            self.job_status_info.update(
                {
                    timeStamp: JobStatusUpdate(
                        Status=None,
                        MinorStatus=None,
                        ApplicationStatus=application_status,
                        Source=self.source,
                    )
                }
            )

    def setJobParameter(self, key: str, value: str):
        """Set job parameter.

        :param par_name: name of the parameter
        :param par_value: value of the parameter
        """
        self.job_parameters.update({key: value})

    def setJobParameters(self, parameters: list[tuple] | dict):
        """Set job parameters for jobID.

        :param parameters: names and values of job parameters: [(name, value), ...] or {name:value, ...}
        """
        if isinstance(parameters, dict):
            self.job_parameters.update(parameters)
        else:
            for pname, pvalue in parameters:
                self.job_parameters.update({pname: pvalue})

    async def sendStoredStatusInfo(self):
        """Send all the accumulated job status information."""
        if not self.job_status_info:
            return
        body = {self.job_id: self.job_status_info}
        ret = await self._client.jobs.set_job_statuses(body)
        if ret.success:
            self.job_status_info = {}
        else:
            raise RuntimeError("Could not set job statuses:", ret)

    async def sendStoredJobParameters(self):
        """Send all the accumulated job parameters information."""
        if not self.job_parameters:
            return
        body = {self.job_id: self.job_parameters}
        ret = await self._client.jobs.patch_metadata(body)
        if ret.success:
            self.job_parameters = {}
        else:
            raise RuntimeError("Could not set job parameters:", ret)

    async def commit(self):
        """Send all the accumulated information."""
        await self.sendStoredStatusInfo()
        await self.sendStoredJobParameters()
