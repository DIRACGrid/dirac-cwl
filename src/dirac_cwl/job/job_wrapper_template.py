#!/usr/bin/env python
"""Job wrapper template for executing CWL jobs."""

import asyncio
import json
import logging
import os
import sys
import tempfile

import DIRAC  # type: ignore[import-untyped]
from cwl_utils.parser import load_document_by_uri
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile
from ruamel.yaml import YAML

if os.getenv("DIRAC_PROTO_LOCAL") != "1":
    DIRAC.initialize()

from dirac_cwl.job.job_wrapper import JobWrapper
from dirac_cwl.submission_models import JobModel


async def main():
    """Execute the job wrapper for a given job model."""
    if len(sys.argv) != 3:
        logging.error("2 arguments required, <json-file> <jobID>")
        sys.exit(1)

    job_id = int(sys.argv[2])

    job_json_file = sys.argv[1]
    job_wrapper = JobWrapper(job_id)
    with open(job_json_file, "r") as file:
        job_model_dict = json.load(file)

    task_dict = job_model_dict["task"]

    with tempfile.NamedTemporaryFile("w+", suffix=".cwl", delete=False) as f:
        YAML().dump(task_dict, f)
        f.flush()
        task_obj = load_document_by_uri(f.name)

    if job_model_dict["input"]:
        cwl_inputs_obj = load_inputfile(job_model_dict["input"]["cwl"])
        job_model_dict["input"]["cwl"] = cwl_inputs_obj
    job_model_dict["task"] = task_obj

    job = JobModel.model_validate(job_model_dict)

    res = await job_wrapper.run_job(job)
    if res:
        logging.info("Job done.")
        return 0
    else:
        logging.info("Job failed.")
        return 1


def setup_diracx() -> None:
    """Get a DiracX client instance with the current user's credentials."""
    import stat
    from pathlib import Path

    from DIRAC import gConfig
    from DIRAC.Core.Security.Locations import getDefaultProxyLocation  # type: ignore[import-untyped]

    diracxUrl = gConfig.getValue("/DiracX/URL")
    if not diracxUrl:
        raise ValueError("Missing mandatory /DiracX/URL configuration")

    os.environ["DIRACX_URL"] = diracxUrl

    proxyLocation = getDefaultProxyLocation()
    diracxToken = DIRAC.Core.Security.DiracX.diracxTokenFromPEM(proxyLocation)
    if not diracxToken:
        raise ValueError(f"No diracx token in the proxy file {proxyLocation}")

    token_file = Path.home() / ".cache" / "diracx" / "credentials.json"
    token_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(token_file, flags=os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode=stat.S_IRUSR | stat.S_IWUSR)
    with open(fd, "w", encoding="utf-8") as fd:
        fd.write(json.dumps(diracxToken))


if __name__ == "__main__":
    if os.getenv("DIRAC_PROTO_LOCAL") != "1":
        setup_diracx()
    sys.exit(asyncio.run(main()))
