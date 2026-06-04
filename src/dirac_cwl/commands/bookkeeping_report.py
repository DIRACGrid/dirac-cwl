"""LHCb command for bookkeeping report file generation based on the XMLSummary and the XML catalog."""

import copy
import logging
import os
from typing import Any, Dict

from DIRAC.Core.Utilities.ReturnValues import SErrorException, returnValueOrRaise
from DIRAC.Workflow.Utilities.Utils import getStepCPUTimes
from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs
from LHCbDIRAC.Workflow.Modules.BookkeepingReport import (
    _generate_xml_object,
    _generateInputFiles,
    _generateOutputFiles,
    _prepare_job_info,
    _process_time,
)
from LHCbDIRAC.Workflow.Modules.ModulesUtilities import getNumberOfProcessorsToUse

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .core import PostProcessCommand
from .workflow_commons import Step, StepStatus, WorkflowCommons

logger = logging.getLogger(__name__)


class BookkeepingReport(PostProcessCommand):
    """Generates a bookkeeping report file based on the XMLSummary and the pool XML catalog."""

    def _execute(self, job_path: os.PathLike, workflow_commons: WorkflowCommons, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param workflow_commons: WorkflowCommons object
        :param kwargs: Additional keyword arguments.
        """
        for step in workflow_commons.steps:
            if workflow_commons.step_status == StepStatus.Failed:
                return

            self._execute_for_step(job_path, workflow_commons, step, **kwargs)

    def _execute_for_step(self, job_path: os.PathLike, workflow_commons: WorkflowCommons, step_commons: Step, **kwargs):
        # Setup variables
        cpu_times: Dict[str, Any] = {}
        if step_commons.start_time:
            cpu_times["StartTime"] = step_commons.start_time
        if step_commons.start_stats:
            cpu_times["StartStats"] = step_commons.start_stats

        exectime, cputime = getStepCPUTimes(cpu_times)

        number_of_processors = workflow_commons.number_of_processors

        if (step_commons.multicore and workflow_commons.multicore) or (
            workflow_commons.job_type.lower() == "user" and workflow_commons.max_number_of_processors
        ):
            number_of_processors = getNumberOfProcessorsToUse(
                workflow_commons.job_id,
                workflow_commons.max_number_of_processors,
            )

        all_outputs = copy.deepcopy(step_commons.outputs)
        all_outputs.extend(step_commons.outputs)

        parameters = {
            "PRODUCTION_ID": workflow_commons.production_id,
            "JOB_ID": workflow_commons.prod_job_id,
            "configVersion": workflow_commons.config_version,
            "outputList": all_outputs,
            "configName": workflow_commons.config_name,
            "outputDataFileMask": workflow_commons.output_data_file_mask,
        }

        if workflow_commons.bookkeeping_lfns and workflow_commons.production_output_data:
            bk_lfns = workflow_commons.bookkeeping_lfns

            if not isinstance(bk_lfns, list):
                bk_lfns = [i.strip() for i in bk_lfns.split(";")]

        else:
            logger.info("BookkeepingLFNs parameters not found, creating on the fly")
            try:
                production_lfns_dict = returnValueOrRaise(
                    constructProductionLFNs(parameters, workflow_commons.bk_client)
                )
            except SErrorException as e:
                logger.error("Could not create production LFNs", exc_info=e)
                raise WorkflowProcessingException(f"Could not create production LFNs: {e}") from e

            bk_lfns = production_lfns_dict["BookkeepingLFNs"]

        ldate, ltime, ldatestart, ltimestart = _process_time(step_commons.start_time)

        # Obtain XMLSummary
        if not step_commons.xf_o:
            step_commons.xf_o = _generate_xml_object(
                step_commons.cleaned_application_name,
                workflow_commons.production_id,
                workflow_commons.prod_job_id,
                step_commons.number,
                step_commons.id,
            )

        info_dict = {
            "exectime": exectime,
            "cputime": cputime,
            "numberOfProcessors": number_of_processors,
            "production_id": workflow_commons.production_id,
            "jobID": workflow_commons.job_id,
            "siteName": workflow_commons.site_name,
            "jobType": workflow_commons.job_type,
            "applicationName": step_commons.application_name,
            "applicationVersion": step_commons.application_version,
            "numberOfEvents": step_commons.number_of_events,
        }

        # Generate job_info object
        job_info = _prepare_job_info(
            info_dict,
            ldatestart,
            ltimestart,
            ldate,
            ltime,
            step_commons.xf_o,
            step_commons.inputs,
            step_commons.id,
            step_commons.bk_id,
            workflow_commons.bk_client,
            workflow_commons.config_name,
            workflow_commons.config_version,
        )

        # Add input files to job_info
        _generateInputFiles(job_info, bk_lfns, step_commons.inputs)

        # Add output files to job_info
        _generateOutputFiles(
            job_info,
            bk_lfns,
            step_commons.event_type,
            step_commons.application_name,
            step_commons.xf_o,
            step_commons.outputs,
            step_commons.inputs,
            step_commons.size,
            step_commons.md5,
            step_commons.guid,
        )

        # Generate SimulationConditions
        if step_commons.application_name == "Gauss":
            job_info.simulation_condition = workflow_commons.sim_description

        # Convert job_info object to XML
        doc = job_info.to_xml()

        # Write to file
        bfilename = f"bookkeeping_{step_commons.id}.xml"
        with open(bfilename, "wb") as bfile:
            bfile.write(doc)
