"""Workflow common values shared between steps."""

from __future__ import annotations

import json
import logging
import os
import shutil
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from DIRAC import siteName
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.Core.Utilities.ReturnValues import SErrorException, returnValueOrRaise
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.TransformationSystem.Client.FileReport import FileReport
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary
from pydantic import BaseModel, ConfigDict, PrivateAttr


class StepStatus(str, Enum):
    """Workflow status."""

    Done = "Done"
    Failed = "Failed"


class WorkflowCommons(BaseModel):
    """Workflow information for command processing."""

    # Mandatory Values
    job_id: int
    job_type: str
    production_id: str
    prod_job_id: str

    inputs: list[str]
    outputs: list[dict[str, Any]]

    number_of_events: int
    event_type: str

    step_id: str
    step_number: int

    config_version: str
    config_name: str

    # Optional values
    executable: str = "gaudirun.py"
    application_name: str = "Unknown"
    application_version: str = "Unknown"

    production_output_data: list[str] = []
    output_data_file_mask: str = ""
    output_data_type: str = ""
    output_data_step: str = ""
    output_SEs: dict[str, str] = {}
    output_mode: str = ""

    log_target_path: str = ""
    log_file_path: str = ""
    log_lfn_path: str = ""
    log_dir: str = ""

    application_log: str = ""
    application_type: str = ""
    cleaned_application_name: str = ""

    number_of_processors: int = 1
    max_number_of_processors: int = 0

    step_proc_pass: str = ""
    run_number: str = "Unknown"
    sim_description: str = "NoSimConditions"

    file_descendents: list[str] = []
    file_size_map: dict[str, str] = {}
    file_md5_map: dict[str, str] = {}
    file_guid_map: dict[str, str] = {}

    bookkeeping_lfns: list[str] = []
    prod_output_lfns: list[str] = []

    file_report_files_dict: dict = {}
    accounting_registers: list = []
    xml_summary_path: str = ""
    request_dict: dict = {}
    files_report_files_dict: dict = {}

    bk_step_id: Optional[int] = None
    start_stats: Optional[tuple] = None
    start_time: Optional[float] = None

    site_name: str = ""
    step_status: StepStatus = StepStatus.Done

    # Private attributes
    _request = PrivateAttr(default=None)
    _failover_request = PrivateAttr(default=None)
    _job_report = PrivateAttr(default=None)
    _file_report = PrivateAttr(default=None)
    _data_manager = PrivateAttr(default=None)
    _bk_client = PrivateAttr(default=None)
    _dsc = PrivateAttr(default=None)
    _xf_o = PrivateAttr(default=None)

    _logger = PrivateAttr(default=logging.getLogger(__name__))

    model_config = ConfigDict(
        validate_assignment=True,
    )

    def __init__(self, **data):
        """WorkflowCommons constructor."""
        super().__init__(**data)

        self.cleaned_application_name = self.application_name.replace("/", "")
        self.site_name = siteName()

        self._request = Request(fromDict=self.request_dict)

        self._failover_request = FailoverTransfer(self.request)

        self._job_report = JobReport(self.job_id)

        self._file_report = FileReport()
        self._file_report.statusDict = self.file_report_files_dict

        self._data_manager = DataManager()
        self._bk_client = BookkeepingClient()
        self._dsc = DataStoreClient()

        if self.xml_summary_path:
            self._xf_o = XMLSummary(self.xml_summary_path)

    def save(self, job_path: os.PathLike, failed: bool = False) -> None:
        """Update the workflow_commons file to accomodate for the new values."""
        wf_path = Path(job_path).joinpath("workflow_commons.json")
        wf_backup = Path(job_path).joinpath("workflow_commons.json.back")

        shutil.move(wf_path, wf_backup)

        if failed:
            self.step_status = StepStatus.Failed

        self.request_dict = json.loads(self.request.toJSON()["Value"])

        try:
            wf_dict = self.model_dump(mode="json")
            with open(wf_path, "w", encoding="utf-8") as f:
                json.dump(wf_dict, f)
        except Exception:
            raise
        finally:
            wf_backup.unlink()

    @classmethod
    def load(cls, job_path: os.PathLike) -> WorkflowCommons:
        """Return a WorkflowCommons containing the values of a workflow_commons.json file.

        :raises: ValidationError
        """
        wf_path = os.path.join(job_path, "workflow_commons.json")

        with open(wf_path, "r", encoding="utf-8") as f:
            wf_dict = json.load(f)

        return cls(**wf_dict)

    # Properties

    @property
    def request(self) -> Request:
        """Request property getter."""
        return self._request

    @request.setter
    def request(self, value: Request) -> None:
        """Request property setter."""
        self._request = value

    @property
    def failover_request(self) -> FailoverTransfer:
        """FailoverTransfer property getter."""
        return self._failover_request

    @property
    def job_report(self) -> JobReport:
        """JobReport property getter."""
        return self._job_report

    @property
    def file_report(self) -> FileReport:
        """FileReport property getter."""
        return self._file_report

    @property
    def data_manager(self) -> DataManager:
        """DataManager property getter."""
        return self._data_manager

    @property
    def bk_client(self) -> BookkeepingClient:
        """BookkeepingClient property getter."""
        return self._bk_client

    @property
    def dsc(self) -> DataStoreClient:
        """DataStoreClient property getter."""
        return self._dsc

    @property
    def xf_o(self) -> XMLSummary:
        """XMLSummary property getter."""
        return self._xf_o

    @xf_o.setter
    def xf_o(self, xf_o: XMLSummary) -> None:
        """XMLSummary property getter."""
        self._xf_o = xf_o

    def generateFailoverFile(self):
        """Create a request.json file."""
        try:
            diset_op = returnValueOrRaise(self.job_report.generateForwardDISET())
        except SErrorException as e:
            self._logger.warning("Could not generate Operation for job report", exc_info=e)

        if diset_op:
            self._logger.info("Populating request with job report information")
            self.request.addOperation(diset_op)

        if len(self.request):
            # Try to optimize the request
            try:
                returnValueOrRaise(self.request.optimize())
            except SErrorException as e:
                self._logger.error("Could not optimize", exc_info=e)
                self._logger.error("Not failing the job because of that, keep going")
            except Exception:
                pass

            # Validate workflow_commons.request
            returnValueOrRaise(RequestValidator().validate(self.request))

            # Get the self.request as a Json
            request_json_content = returnValueOrRaise(self.request.toJSON())

            # Write it
            fname = f"{self.production_id}_{self.prod_job_id}_request.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(request_json_content, f)

        if self.accounting_registers:
            for register in self.accounting_registers:
                self.dsc.addRegister(register)
            self.dsc.commit()
