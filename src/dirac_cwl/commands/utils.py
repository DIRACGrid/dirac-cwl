"""."""

import json
import os
import shutil

from DIRAC import siteName
from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK

from dirac_cwl.core.exceptions import WorkflowProcessingException


def prepare_lhcb_workflow_commons(workflow_commons_path, extra_mandatory_values=[], extra_default_values={}):
    """Return a dictionary containing the values of a workflow_commons.json file.

    Also performs a series of checks to ensure everything is in order.
    """
    if not os.path.exists(workflow_commons_path):
        raise WorkflowProcessingException(f"{workflow_commons_path} file not found")

    with open(workflow_commons_path, "r", encoding="utf-8") as f:
        workflow_commons = json.load(f)

    if not workflow_commons:
        raise WorkflowProcessingException(f"{workflow_commons_path} cannot be empty")

    mandatory_values = [
        "job_id",
        "job_type",
        "production_id",
        "prod_job_id",
        "number_of_events",
        "application_name",
        "application_version",
        "inputs",
        "outputs",  # outputList
        "executable",
        "command_id",  # StepID
        "command_number",
    ]

    mandatory_values.extend(extra_mandatory_values)
    missing_values = []

    for value in mandatory_values:
        if value not in workflow_commons:
            missing_values.append(value)

    if missing_values:
        raise WorkflowProcessingException(
            f"The following values are missing in workflow_commons.json: {missing_values}"
        )

    commons_defaults = {
        "output_data_file_mask": "",
        "run_metadata": {},
        "log_target_path": "",
        "production_output_data": [],
        "CPUe": 0,
        "max_number_of_events": "0",
        "output_data_type": None,
        "application_log": "",
        "application_type": None,
        "options_file": None,
        "options_line": None,
        "extra_packages": "",
        "multi_core": False,
        "max_number_of_processors": None,
        "system_config": None,
        "mcTCK": None,
        "condDB_tag": None,
        "DQ_tag": None,
        "step_status": S_OK(),
        "config_name": None,
        "config_version": None,
        "request_dict": {},
        "file_report_files_dict": {},
        "number_of_processors": 1,
    }

    for k, v in extra_default_values.items():
        if k not in commons_defaults:
            commons_defaults[k] = v

    for k, v in commons_defaults.items():
        if k not in workflow_commons:
            workflow_commons[k] = v

    cleaned_application_name = workflow_commons["application_name"].replace("/", "")
    workflow_commons["cleaned_application_name"] = cleaned_application_name

    workflow_commons["site_name"] = siteName()

    return workflow_commons


def save_workflow_commons(wf_commons, wf_file_path, request=None, failed=False):
    """Update the workflow_commons file to accomodate for the new values.

    Ensures that no data is lost during the update by creating a backup.
    """
    if not (os.path.exists(wf_file_path) and os.path.isfile(wf_file_path)):
        raise WorkflowProcessingException(f"Workflow Commons file '{wf_file_path}' not found")

    wf_filename = os.path.basename(wf_file_path)
    wf_backup = f"{wf_filename}.bak"

    shutil.move(wf_file_path, wf_backup)

    if failed:
        wf_commons["step_status"] = S_ERROR()

    if request:
        wf_commons["request_dict"] = json.loads(request.toJSON()["Value"])

    try:
        with open(wf_file_path, "x", encoding="utf-8") as f:
            json.dump(wf_commons, f)
    except Exception:
        os.unlink(wf_file_path)
        shutil.copy2(wf_backup, wf_file_path)
        return False

    finally:
        os.unlink(wf_backup)

    return True
