"""Workflow common values shared between steps."""

from __future__ import annotations

import json
import logging
import os
import shutil
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Union

from DIRAC import siteName
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.RequestManagementSystem.Client.Request import Request
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

logger = logging.getLogger(__name__)


class StepStatus(str, Enum):
    """Workflow status."""

    Done = "Done"
    Failed = "Failed"


class Step(BaseModel):
    """Execution step information."""

    id: str
    name: str
    number: int

    executable: str = "gaudirun.py"

    application_name: Optional[str] = "Unknown"
    cleaned_application_name: str = ""
    application_version: str = "Unknown"
    application_log: str = ""
    application_type: str = ""

    event_type: str = ""
    number_of_events: int = 0
    event_timeout: Optional[int] = None

    extra_packages: Optional[str] = ""
    proc_pass: str = ""
    bk_id: str = ""
    multicore: bool = False
    mc_tck: str = ""
    system_config: str = ""

    dddb_tag: str = ""
    conddb_tag: str = ""
    dq_tag: str = ""

    inputs: list[str] = []
    outputs: list[dict[str, Any]] = []

    input_data_type: str = ""

    options_file: str = ""
    options_line: str = ""
    extra_options_line: str = ""
    options_format: str = ""

    size: dict = {}
    md5: dict = {}
    guid: dict = {}

    start_time: Optional[float] = None
    start_stats: Optional[tuple] = None

    # To be built if certain conditions are met
    # > If (wf_c.production_id && wf_c.job_id && self.name && self.inputs)
    output_file_prefix: str = ""
    xml_summary_path: str = ""
    histo_name: str = "Hist.root"

    # Private Attributes
    _xf_o: Optional[XMLSummary] = PrivateAttr(default=None)

    def __init__(self, **data):
        """StepCommons constructor."""
        super().__init__(**data)

        if self.application_name:
            self.cleaned_application_name = self.application_name.replace("/", "")

        if self.xml_summary_path:
            self._xf_o = XMLSummary(self.xml_summary_path)

    @property
    def xf_o(self) -> XMLSummary:
        """Xml Summary getter."""
        return self._xf_o

    @xf_o.setter
    def xf_o(self, value: XMLSummary) -> None:
        """Xml Summary getter."""
        self._xf_o = value


class WorkflowCommons(BaseModel):
    """Workflow information for command processing."""

    # Mandatory Values
    job_id: int
    job_type: str
    production_id: str
    prod_job_id: str

    inputs: list[str] = []
    outputs: list[dict[str, Any]] = []

    config_version: str
    config_name: str

    steps: list[Step] = []

    # Optional values
    production_output_data: list[str] = []
    output_data_file_mask: str = ""
    output_data_type: str = ""
    output_SEs: dict[str, list[str]] = {}  # output -> SE list
    output_mode: str = ""
    output_data_step: str = ""

    log_target_path: str = ""
    log_file_path: str = ""
    log_lfn_path: str = ""
    log_dir: str = ""

    number_of_processors: int = 1
    max_number_of_processors: Optional[int] = None

    run_number: str = "Unknown"
    sim_description: str = "NoSimConditions"

    bookkeeping_lfns: list[str] = []
    prod_output_lfns: list[str] = []

    file_descendents: list[str] = []
    file_report_files_dict: dict = {}
    accounting_registers: list = []
    xml_summary_paths: dict[str, str] = {}
    request_dict: dict = {}

    site_name: str = Field(default_factory=siteName)
    multicore: bool = False

    step_status: StepStatus = StepStatus.Done

    _logger = PrivateAttr(default=logging.getLogger(__name__))

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    def __init__(self, **data):
        """WorkflowCommons constructor."""
        super().__init__(**data)

    def save(
        self,
        job_path: os.PathLike,
        request: Union[Request, None] = None,
        dsc: Union[DataStoreClient, None] = None,
        failed: bool = False,
    ) -> None:
        """Update the workflow_commons file to accomodate for the new values."""
        logger.info("Saving workflow commons json file")
        wf_path = Path(job_path).joinpath("workflow_commons.json")
        wf_backup = Path(job_path).joinpath("workflow_commons.json.back")

        if os.path.exists(wf_path):
            shutil.move(wf_path, wf_backup)

        if failed:
            self.step_status = StepStatus.Failed

        if request:
            self.request_dict = json.loads(request.toJSON()["Value"])
        if dsc:
            self.accounting_registers.extend(dsc._DataStoreClient__registersList)

        try:
            wf_dict = self.model_dump(mode="json")
            with open(wf_path, "w", encoding="utf-8") as f:
                json.dump(wf_dict, f)
        except Exception as e:
            self._logger.exception("Failed to save the workflows commons in a file", exc_info=e)
            raise
        finally:
            wf_backup.unlink(missing_ok=True)

    @classmethod
    def load(cls, job_path: os.PathLike) -> WorkflowCommons:
        """Return a WorkflowCommons containing the values of a workflow_commons.json file.

        :raises: ValidationError
        """
        wf_path = os.path.join(job_path, "workflow_commons.json")

        with open(wf_path, "r", encoding="utf-8") as f:
            wf_dict = json.load(f)

        return cls(**wf_dict)
