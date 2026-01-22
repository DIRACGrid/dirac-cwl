#!/usr/bin/env python
"""Job wrapper for executing CWL workflows with DIRAC."""

import json
import logging
import random
import shutil
import subprocess
from pathlib import Path
from typing import Sequence, cast

from cwl_utils.parser import (
    save,
)
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ExpressionTool,
    File,
    Saveable,
    Workflow,
)
from DIRACCommon.Core.Utilities.ReturnValues import (  # type: ignore[import-untyped]
    returnValueOrRaise,
)
from rich.text import Text
from ruamel.yaml import YAML

from dirac_cwl_proto.core.utility import get_lfns
from dirac_cwl_proto.data_management_mocks.sandbox import download_sandbox, upload_sandbox
from dirac_cwl_proto.execution_hooks import ExecutionHooksHint
from dirac_cwl_proto.execution_hooks.core import ExecutionHooksBasePlugin
from dirac_cwl_proto.submission_models import (
    JobInputModel,
    JobModel,
)

# -----------------------------------------------------------------------------
# JobWrapper
# -----------------------------------------------------------------------------

logger = logging.getLogger(__name__)


class JobWrapper:
    """Job Wrapper for the execution hook."""

    def __init__(self) -> None:
        """Initialize the job wrapper."""
        self.execution_hooks_plugin: ExecutionHooksBasePlugin | None = None
        self.job_path: Path = Path()

    def __download_input_sandbox(self, arguments: JobInputModel, job_path: Path) -> None:
        """Download the files from the sandbox store.

        :param arguments: Job input model containing sandbox information.
        :param job_path: Path to the job working directory.
        """
        assert arguments.sandbox is not None
        if not self.execution_hooks_plugin:
            raise RuntimeError("Could not download sandboxes")
        for sandbox in arguments.sandbox:
            download_sandbox(sandbox, job_path)

    def __upload_output_sandbox(
        self,
        outputs: dict[str, str | Path | Sequence[str | Path]],
    ):
        if not self.execution_hooks_plugin:
            raise RuntimeError("Could not upload sandbox : Execution hook is not defined.")
        for output_name, src_path in outputs.items():
            if self.execution_hooks_plugin.output_sandbox and output_name in self.execution_hooks_plugin.output_sandbox:
                if isinstance(src_path, Path) or isinstance(src_path, str):
                    src_path = [src_path]

                sb_path = Path(f"sandboxstore/{upload_sandbox(src_path)}")
                if not sb_path.exists():
                    raise RuntimeError(f"Sandbox {sb_path} does not exist")
                logger.info("Successfully stored output %s in Sandbox %s", output_name, sb_path)

    def __download_input_data(self, inputs: JobInputModel, job_path: Path) -> dict[str, Path | list[Path]]:
        """Download LFNs into the job working directory.

        :param JobInputModel inputs:
            The job input model containing ``lfns_input``, a mapping from input names to one or more LFN paths.
        :param Path job_path:
            Path to the job working directory where files will be copied.

        :return dict[str, Path | list[Path]]:
            A dictionary mapping each input name to the corresponding downloaded
            file path(s) located in the working directory.
        """
        new_paths: dict[str, Path | list[Path]] = {}
        if not self.execution_hooks_plugin:
            raise RuntimeWarning("Could not download input data: Execution hook is not defined.")

        lfns_inputs = get_lfns(inputs.cwl)

        if lfns_inputs:
            for input_name, lfns in lfns_inputs.items():
                res = returnValueOrRaise(self.execution_hooks_plugin._datamanager.getFile(lfns, str(job_path)))
                if res["Failed"]:
                    raise RuntimeError(f"Could not get files : {res['Failed']}")
                paths = res["Successful"]
                if paths and isinstance(lfns, list):
                    new_paths[input_name] = [Path(paths[lfn]).relative_to(job_path.resolve()) for lfn in paths]
                elif paths and isinstance(lfns, str):
                    new_paths[input_name] = Path(paths[lfns]).relative_to(job_path.resolve())
        return new_paths

    def __update_inputs(self, inputs: JobInputModel, updates: dict[str, Path | list[Path]]):
        """Update CWL job inputs with new file paths.

        This method updates the `inputs.cwl` object by replacing or adding
        file paths for each input specified in `updates`. It supports both
        single files and lists of files.

        :param inputs: The job input model whose `cwl` dictionary will be updated.
        :type inputs: JobInputModel
        :param updates: Dictionary mapping input names to their corresponding local file
            paths. Each value can be a single `Path` or a list of `Path` objects.
        :type updates: dict[str, Path | list[Path]]

        .. note::
           This method is typically called after downloading LFNs
           using `download_lfns` to ensure that the CWL job inputs reference
           the correct local files.
        """
        for input_name, path in updates.items():
            if isinstance(path, Path):
                inputs.cwl[input_name] = File(path=str(path))
            else:
                inputs.cwl[input_name] = []
                for p in path:
                    inputs.cwl[input_name].append(File(path=str(p)))

    def __parse_output_filepaths(self, stdout: str) -> dict[str, str | Path | Sequence[str | Path]]:
        """Get the outputted filepaths per output.

        :param str stdout:
            The console output of the the job

        :return dict[str, list[str]]:
            The dict of the list of filepaths for each output
        """
        outputted_files: dict[str, str | Path | Sequence[str | Path]] = {}
        outputs = json.loads(stdout)
        for output, files in outputs.items():
            if not files:
                continue
            if not isinstance(files, list):
                files = [files]
            file_paths = []
            for file in files:
                if file:
                    file_paths.append(str(file["path"]))
            outputted_files[output] = file_paths
        return outputted_files

    def _pre_process(
        self,
        executable: CommandLineTool | Workflow | ExpressionTool,
        arguments: JobInputModel | None,
    ) -> list[str]:
        """
        Pre-process the job before execution.

        :return: True if the job is pre-processed successfully, False otherwise
        """
        logger = logging.getLogger("JobWrapper - Pre-process")

        # Prepare the task for cwltool
        logger.info("Preparing the task for cwltool...")
        command = ["cwltool", "--parallel"]

        task_dict = save(executable)
        task_path = self.job_path / "task.cwl"
        with open(task_path, "w") as task_file:
            YAML().dump(task_dict, task_file)
        command.append(str(task_path.name))

        if arguments:
            if arguments.sandbox:
                # Download the files from the sandbox store
                logger.info("Downloading the files from the sandbox store...")
                self.__download_input_sandbox(arguments, self.job_path)
                logger.info("Files downloaded successfully!")

            updates = self.__download_input_data(arguments, self.job_path)
            self.__update_inputs(arguments, updates)

            logger.info("Preparing the parameters for cwltool...")
            parameter_dict = save(cast(Saveable, arguments.cwl))
            parameter_path = self.job_path / "parameter.cwl"
            with open(parameter_path, "w") as parameter_file:
                YAML().dump(parameter_dict, parameter_file)
            command.append(str(parameter_path.name))

        if self.execution_hooks_plugin:
            return self.execution_hooks_plugin.pre_process(executable, arguments, self.job_path, command)

        return command

    def _post_process(
        self,
        status: int,
        stdout: str,
        stderr: str,
    ):
        """
        Post-process the job after execution.

        :return: True if the job is post-processed successfully, False otherwise
        """
        logger = logging.getLogger("JobWrapper - Post-process")
        if status != 0:
            raise RuntimeError(f"Error {status} during the task execution.")

        logger.info(stdout)
        logger.info(stderr)

        outputs = self.__parse_output_filepaths(stdout)

        if self.execution_hooks_plugin:
            return self.execution_hooks_plugin.post_process(self.job_path, outputs=outputs)

        self.__upload_output_sandbox(outputs=outputs)

        return True

    def run_job(self, job: JobModel) -> bool:
        """Execute a given CWL workflow using cwltool.

        This is the equivalent of the DIRAC JobWrapper.

        :param job: The job model containing workflow and inputs.
        :return: True if the job is executed successfully, False otherwise.
        """
        logger = logging.getLogger("JobWrapper")
        # Instantiate runtime metadata from the serializable descriptor and
        # the job context so implementations can access task inputs/overrides.
        job_execution_hooks = ExecutionHooksHint.from_cwl(job.task)
        self.execution_hooks_plugin = job_execution_hooks.to_runtime(job) if job_execution_hooks else None

        # Isolate the job in a specific directory
        self.job_path = Path(".") / "workernode" / f"{random.randint(1000, 9999)}"
        self.job_path.mkdir(parents=True, exist_ok=True)

        try:
            # Pre-process the job
            logger.info("Pre-processing Task...")
            command = self._pre_process(job.task, job.input)
            logger.info("Task pre-processed successfully!")

            # Execute the task
            logger.info("Executing Task: %s", command)
            result = subprocess.run(command, capture_output=True, text=True, cwd=self.job_path)

            if result.returncode != 0:
                logger.error("Error in executing workflow:\n%s", Text.from_ansi(result.stderr))
                return False
            logger.info("Task executed successfully!")

            # Post-process the job
            logger.info("Post-processing Task...")
            if self._post_process(
                result.returncode,
                result.stdout,
                result.stderr,
            ):
                logger.info("Task post-processed successfully!")
                return True
            logger.error("Failed to post-process Task")
            return False

        except Exception:
            logger.exception("JobWrapper: Failed to execute workflow")
            return False
        finally:
            # Clean up
            if self.job_path.exists():
                shutil.rmtree(self.job_path)
