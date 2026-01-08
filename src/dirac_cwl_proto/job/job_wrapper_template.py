#!/usr/bin/env python
"""Job wrapper template for executing CWL jobs."""

import json
import logging
import sys
import tempfile

from cwl_utils.parser import load_document_by_uri
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile
from ruamel.yaml import YAML

from dirac_cwl_proto.job.job_wrapper import JobWrapper
from dirac_cwl_proto.submission_models import JobModel


def main():
    """Execute the job wrapper for a given job model."""
    if len(sys.argv) != 2:
        logging.error("1 argument is required")
        sys.exit(1)

    job_json_file = sys.argv[1]
    job_wrapper = JobWrapper()
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

    res = job_wrapper.run_job(job)
    if res:
        logging.info("Job done.")
    else:
        logging.info("Job failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
