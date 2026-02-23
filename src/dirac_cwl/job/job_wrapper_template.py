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
    await job_wrapper.initialize()
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


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
