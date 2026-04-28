"""LHCb command for bookkeeping report file generation based on the XMLSummary and the XML catalog."""

import os

from DIRAC.Workflow.Utilities.Utils import getStepCPUTimes
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary
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
from .utils import prepare_lhcb_workflow_commons


class BookeepingReport(PostProcessCommand):
    """Generates a bookkeeping report file based on the XMLSummary and the pool XML catalog."""

    def execute(self, job_path, **kwargs):
        """Execute the command.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        # Obtain Workflow Commons
        workflow_commons_path = kwargs.get("workflow-commons-path", os.path.join(job_path, "workflow_commons.json"))

        workflow_commons = prepare_lhcb_workflow_commons(
            workflow_commons_path,
            extra_mandatory_values=[
                "bk_step_id",
            ],
            extra_default_values={
                "bookkeeping_LFNs": [],
                "size": {},
                "md5": {},
                "guid": {},
                "sim_description": "NoSimConditions",
            },
        )

        if not workflow_commons["step_status"]["OK"]:
            return

        # Setup variables
        start_time = workflow_commons.get("start_time", None)

        cpu_times = {}
        if start_time:
            cpu_times["StartTime"] = start_time
        if "start_stats" in workflow_commons:
            cpu_times["StartStats"] = workflow_commons["start_stats"]

        exectime, cputime = getStepCPUTimes(cpu_times)

        number_of_processors = getNumberOfProcessorsToUse(
            workflow_commons["job_id"], workflow_commons["max_number_of_processors"]
        )

        bk_client = BookkeepingClient()

        parameters = {
            "PRODUCTION_ID": workflow_commons["production_id"],
            "JOB_ID": workflow_commons["prod_job_id"],
            "configVersion": workflow_commons["config_version"],
            "outputList": workflow_commons["outputs"],
            "configName": workflow_commons["config_name"],
            "outputDataFileMask": workflow_commons["output_data_file_mask"],
        }

        if "bookkeeping_LFNs" in workflow_commons and "production_output_data" in workflow_commons:
            bk_lfns = workflow_commons["bookkeeping_LFNs"]

            if not isinstance(bk_lfns, list):
                bk_lfns = [i.strip() for i in bk_lfns.split(";")]

        else:
            result = constructProductionLFNs(parameters, bk_client)
            if not result["OK"]:
                raise WorkflowProcessingException("Could not create production LFNs")

            bk_lfns = result["Value"]["BookkeepingLFNs"]

        ldate, ltime, ldatestart, ltimestart = _process_time(start_time)

        # Obtain XMLSummary
        if "xml_summary_path" in workflow_commons:
            xf_o = XMLSummary(workflow_commons["xml_summary_path"])
        else:
            xf_o = _generate_xml_object(
                workflow_commons["cleaned_application_name"],
                workflow_commons["production_id"],
                workflow_commons["prod_job_id"],
                workflow_commons["command_number"],
                workflow_commons["command_id"],
            )

        info_dict = {
            "exectime": exectime,
            "cputime": cputime,
            "numberOfProcessors": number_of_processors,
            "production_id": workflow_commons["production_id"],
            "jobID": workflow_commons["job_id"],
            "siteName": workflow_commons["site_name"],
            "jobType": workflow_commons["job_type"],
            "applicationName": workflow_commons["application_name"],
            "applicationVersion": workflow_commons["application_version"],
            "numberOfEvents": workflow_commons["number_of_events"],
        }

        # Generate job_info object
        job_info = _prepare_job_info(
            info_dict,
            ldatestart,
            ltimestart,
            ldate,
            ltime,
            xf_o,
            workflow_commons["inputs"],
            workflow_commons["command_id"],
            workflow_commons["bk_step_id"],
            bk_client,
            workflow_commons["config_name"],
            workflow_commons["config_version"],
        )

        # Add input files to job_info
        _generateInputFiles(job_info, bk_lfns, workflow_commons["inputs"])

        # Add output files to job_info
        _generateOutputFiles(
            job_info,
            bk_lfns,
            workflow_commons["event_type"],
            workflow_commons["application_name"],
            xf_o,
            workflow_commons["outputs"],
            workflow_commons["inputs"],
        )

        # Generate SimulationConditions
        if workflow_commons["application_name"] == "Gauss":
            job_info.simulation_condition = workflow_commons["sim_description"]

        # Convert job_info object to XML
        doc = job_info.to_xml()

        # Write to file
        bfilename = f"bookkeeping_{workflow_commons['command_id']}.xml"
        with open(bfilename, "wb") as bfile:
            bfile.write(doc)
