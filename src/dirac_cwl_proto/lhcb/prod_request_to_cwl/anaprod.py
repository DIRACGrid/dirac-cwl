###############################################################################
# (c) Copyright 2025 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
"""Converter for LHCb Analysis Production requests to CWL workflows.

This converter takes analysis production request YAML files and converts them
into CWL format, handling DaVinci analysis jobs and ROOT file merging.

The converter:
- Parses AnalysisProduction YAML files
- Generates configuration for DaVinci and merge steps
- Creates CWL workflow with CommandLineTool for each step
- Handles input dataset queries from the Bookkeeping
"""

import json
from pathlib import Path
from typing import Any

import yaml
from cwl_utils.parser.cwl_v1_2 import (
    CommandInputParameter,
    CommandLineBinding,
    CommandLineTool,
    CommandOutputBinding,
    CommandOutputParameter,
    Dirent,
    InitialWorkDirRequirement,
    InlineJavascriptRequirement,
    MultipleInputFeatureRequirement,
    ResourceRequirement,
    StepInputExpressionRequirement,
    SubworkflowFeatureRequirement,
    Workflow,
    WorkflowInputParameter,
    WorkflowOutputParameter,
    WorkflowStep,
    WorkflowStepInput,
    WorkflowStepOutput,
)
from ruamel.yaml.scalarstring import LiteralScalarString

from dirac_cwl_proto.lhcb.prod_request_to_cwl.simulation import (
    _make_case_insensitive_glob,
)

from .common import (
    build_transformation_hints,
    sanitize_step_name,
)


def fromProductionRequestYAMLToCWL(
    yaml_path: Path, production_name: str | None = None
) -> tuple[Workflow, dict[str, Any], dict[str, Any]]:
    """
    Convert an LHCb Analysis Production Request YAML file into a CWL Workflow.

    :param yaml_path: Path to the production request YAML file
    :param production_name: Name of the production to convert (if multiple in file)
    :return: Tuple of (CWL Workflow, CWL inputs dict, production metadata dict)
    """
    # Load and parse YAML
    with open(yaml_path, "r") as f:
        productions_data = yaml.safe_load(f)

    # Handle multiple productions in one file
    if not isinstance(productions_data, list):
        productions_data = [productions_data]

    # Find the requested production
    production_dict = None
    if production_name:
        for prod in productions_data:
            if prod.get("name") == production_name:
                production_dict = prod
                break
        if not production_dict:
            raise ValueError(f"Production '{production_name}' not found in {yaml_path}")
    else:
        if len(productions_data) > 1:
            names = [p.get("name") for p in productions_data]
            raise ValueError(f"Multiple productions found, please specify one: {names}")
        production_dict = productions_data[0]

    # Validate it's an analysis production
    if production_dict.get("type") != "AnalysisProduction":
        raise ValueError(
            f"Expected AnalysisProduction, got {production_dict.get('type')}"
        )

    # Build the CWL workflow
    return _buildCWLWorkflow(production_dict)


def _buildCWLWorkflow(
    production: dict[str, Any]
) -> tuple[Workflow, dict[str, Any], dict[str, Any]]:
    """Build a CWL workflow from an analysis production dictionary.

    The workflow is structured with transformations as sub-workflows:
    - Main workflow has one step per transformation
    - Each transformation is a sub-workflow containing the actual processing steps
    """

    production_name = production.get("name", "unknown_production")
    steps = production.get("steps", [])
    submission_info = production.get("submission_info", {})
    transforms = submission_info.get("transforms", [])

    # Define workflow inputs
    workflow_inputs = _getWorkflowInputs(production)

    # If no transforms defined, fall back to single transformation containing all steps
    if not transforms:
        transforms = [
            {
                "steps": list(range(len(steps))),  # 0-indexed
                "type": "WGProduction",
            }
        ]

    # Build transformation sub-workflows
    transformation_workflows = []
    transformation_names = []

    for transform_index, transform in enumerate(transforms):
        transform_name = f"transformation_{transform_index + 1}"
        transformation_names.append(transform_name)

        # Build transformation sub-workflow
        transformation_workflow = _buildTransformationWorkflow(
            production, transform, transform_index, workflow_inputs
        )
        transformation_workflows.append(transformation_workflow)

    # Build main workflow steps (one per transformation)
    main_workflow_steps = []
    for transform_index, (transform_name, transform_workflow, transform) in enumerate(
        zip(transformation_names, transformation_workflows, transforms)
    ):
        main_step = _buildMainWorkflowStep(
            transform_name,
            transform_workflow,
            transform,
            transform_index,
            workflow_inputs,
            transformation_names,
        )
        main_workflow_steps.append(main_step)

    # Define workflow outputs from the last transformation
    workflow_outputs = _getMainWorkflowOutputs(steps, transforms, transformation_names)

    # Create resource requirements
    resource_requirement = ResourceRequirement(
        coresMin=1,
        ramMin=2048,
    )

    # Build detailed documentation
    input_dataset = production.get("input_dataset", {})
    conditions_dict = input_dataset.get("conditions_dict", {})

    doc_lines = [
        f"LHCb Analysis Production Workflow: {production_name}",
        "",
        "Metadata:",
    ]

    # Add input dataset information
    if input_dataset:
        doc_lines.append("  Input Dataset:")
        if "event_type" in input_dataset:
            doc_lines.append(f"    Event Type: {input_dataset['event_type']}")
        if conditions_dict:
            if "inProPass" in conditions_dict:
                doc_lines.append(f"    Processing Pass: {conditions_dict['inProPass']}")
            if "inFileType" in conditions_dict:
                doc_lines.append(f"    File Type: {conditions_dict['inFileType']}")
            if "configName" in conditions_dict and "configVersion" in conditions_dict:
                doc_lines.append(
                    f"    Config: {conditions_dict['configName']}/{conditions_dict['configVersion']}"
                )
        if "conditions_description" in input_dataset:
            doc_lines.append(
                f"    Conditions: {input_dataset['conditions_description']}"
            )

    # Add workflow structure info
    doc_lines.append("")
    doc_lines.append(f"This workflow contains {len(transforms)} transformation(s).")
    doc_lines.append("")
    doc_lines.append("Generated by ProductionRequestToCWL converter")

    workflow_doc = "\n".join(doc_lines)

    # Create main workflow with extension fields for namespaces
    extension_fields = {
        "$namespaces": {"dirac": "../../../schemas/dirac-metadata.json#/$defs/"},
        "$schemas": ["../../../schemas/dirac-metadata.json"],
    }

    # Build hints for the main workflow
    hints = []

    # Add input dataset as a machine-readable hint for BK query
    if input_dataset:
        input_dataset_hint = {
            "class": "dirac:inputDataset",
            "event_type": input_dataset.get("event_type"),
            "conditions_dict": conditions_dict,
            "conditions_description": input_dataset.get("conditions_description"),
        }

        # Add launch_parameters if present
        launch_params = input_dataset.get("launch_parameters", {})
        if launch_params:
            input_dataset_hint["launch_parameters"] = launch_params

        hints.append(input_dataset_hint)

    workflow = Workflow(
        id=sanitize_step_name(production_name) or "analysis_production",
        label=production_name,
        doc=LiteralScalarString(workflow_doc),
        cwlVersion="v1.2",
        inputs=list(workflow_inputs.values()),
        outputs=workflow_outputs,
        steps=main_workflow_steps,
        requirements=[
            InlineJavascriptRequirement(),
            SubworkflowFeatureRequirement(),
            StepInputExpressionRequirement(),
            MultipleInputFeatureRequirement(),
            resource_requirement,
        ],
        hints=hints,
        extension_fields=extension_fields,
    )

    # Generate static inputs
    static_inputs = _getWorkflowStaticInputs(production)

    # Production metadata
    metadata = {
        "production_name": production_name,
        "production_type": "AnalysisProduction",
        "wg": production.get("wg"),
        "input_dataset": production.get("input_dataset"),
    }

    return workflow, static_inputs, metadata


def _getWorkflowInputs(production: dict[str, Any]) -> dict[str, WorkflowInputParameter]:
    """Define workflow-level input parameters for analysis production."""

    inputs = {}

    # Production identification inputs
    inputs["production-id"] = WorkflowInputParameter(
        id="production-id",
        type_="int",
        doc="Production ID",
        default=12345,
    )

    inputs["prod-job-id"] = WorkflowInputParameter(
        id="prod-job-id",
        type_="int",
        doc="Production Job ID",
        default=6789,
    )

    # Input data - for analysis productions, this comes from BK query
    inputs["input-data"] = WorkflowInputParameter(
        id="input-data",
        type_="File[]",
        doc="Input data files from Bookkeeping query",
    )

    return inputs


def _buildTransformationWorkflow(
    production: dict[str, Any],
    transform: dict[str, Any],
    transform_index: int,
    production_inputs: dict[str, WorkflowInputParameter],
) -> Workflow:
    """Build a CWL sub-workflow for a single transformation.

    A transformation contains one or more processing steps that run within one job.

    :param production: The full production dictionary
    :param transform: The transformation definition from submission_info
    :param transform_index: Index of this transformation (0-based)
    :param production_inputs: Input parameters from the production level
    :return: A Workflow representing this transformation
    """
    steps = production.get("steps", [])
    step_indices = transform.get("steps", [])  # Already 0-indexed in YAML

    # Get the actual steps for this transformation
    transform_steps = [steps[idx] for idx in step_indices]

    # Build transformation-level inputs
    transformation_inputs = {}

    # Always include common inputs
    for input_id in ["production-id", "prod-job-id"]:
        if input_id in production_inputs:
            transformation_inputs[input_id] = production_inputs[input_id]

    # For non-first transformations, add input-data parameter to receive outputs from previous transformation
    # For first transformation, input-data comes from BK query
    if transform_index == 0:
        # First transformation receives input-data from workflow level
        transformation_inputs["input-data"] = production_inputs["input-data"]
    else:
        # Non-first transformations receive input-data from previous transformation
        transformation_inputs["input-data"] = WorkflowInputParameter(
            id="input-data",
            type_="File[]",
            doc="Input data files from previous transformation",
        )

    # Build CWL steps for this transformation
    cwl_steps = []
    step_names = []
    for local_step_index, global_step_index in enumerate(step_indices):
        step = steps[global_step_index]
        step_name = sanitize_step_name(step.get("name", f"step_{global_step_index}"))
        step_names.append(step_name)

        # Build the CommandLineTool for this step
        # Determine if this step receives input data:
        # - If not first step in transformation (local_step_index > 0): receives from previous step
        # - If first step (local_step_index == 0): receives from transformation input-data parameter
        has_input_data = local_step_index > 0 or "input-data" in transformation_inputs
        tool = _buildCommandLineTool(
            production,
            step,
            global_step_index,
            transform_index,
            len(step_indices),
            has_input_data,
        )

        # Build workflow step inputs
        step_inputs = _buildStepInputs(
            step,
            local_step_index,
            global_step_index,
            transformation_inputs,
            step_names,
            step_indices,
        )

        # Build workflow step outputs
        step_outputs = _buildStepOutputs(step)

        # Create workflow step
        cwl_step = WorkflowStep(
            id=step_name,
            run=tool,
            in_=step_inputs,
            out=step_outputs,
        )
        cwl_steps.append(cwl_step)

    # Build transformation outputs
    transformation_outputs = _getTransformationOutputs(steps, transform, step_names)

    # Build transformation hints
    hints = build_transformation_hints(transform)

    # Create transformation sub-workflow
    transformation_workflow = Workflow(
        id=f"transformation_{transform_index + 1}",
        label=f"Transformation {transform_index + 1}",
        doc=f"Transformation {transform_index + 1}: {transform.get('type', 'Processing')}\nContains {len(step_indices)} step(s)",
        cwlVersion="v1.2",
        inputs=list(transformation_inputs.values()),
        outputs=transformation_outputs,
        steps=cwl_steps,
        requirements=[
            InlineJavascriptRequirement(),
            StepInputExpressionRequirement(),
            MultipleInputFeatureRequirement(),
        ],
        hints=hints,
    )

    return transformation_workflow


def _buildCommandLineTool(
    production: dict[str, Any],
    step: dict[str, Any],
    step_index: int,
    transform_index: int,
    total_steps_in_transform: int,
    has_input_data: bool,
) -> CommandLineTool:
    """Build a CommandLineTool for an analysis production step.

    :param production: The full production dictionary
    :param step: The step definition
    :param step_index: Global step index (0-based)
    :param transform_index: Index of the transformation this step belongs to
    :param total_steps_in_transform: Total number of steps in this transformation
    :param has_input_data: Whether this step receives input data
    :return: A CommandLineTool for this step
    """
    # Generate prodConf for this step
    prodconf = _generateProdConf(production, step, step_index)

    step_name = sanitize_step_name(step.get("name", f"step_{step_index}"))
    application = step.get("application", {})

    if isinstance(application, dict):
        app_name = application.get("name", "unknown")
        app_version = application.get("version", "")
    else:
        # Sometimes application is just a string like "DaVinci/v44r11p6"
        parts = application.split("/")
        app_name = parts[0] if parts else "unknown"
        app_version = parts[1] if len(parts) > 1 else ""

    # Determine if this is a merge step
    is_merge = step.get("options", {}).get("entrypoint") == "LbExec:skim_and_merge"

    # Build input parameters
    input_parameters = _buildInputParameters(step, step_index, is_merge, has_input_data)

    # Build output parameters
    output_parameters = _buildOutputParameters(step)

    # Build InitialWorkDirRequirement with prodConf and input files manifest
    prodconf_filename = f"prodConf_{step_index + 1}.json"
    initial_workdir_listing = [
        Dirent(
            entryname=prodconf_filename,
            entry=LiteralScalarString(json.dumps(prodconf, indent=2)),
        )
    ]

    # Add input files manifest if this step has input-data
    # This manifest will contain one file path per line
    if "input-data" in [p.id for p in input_parameters]:
        input_files_manifest = f"inputFiles_{step_index + 1}.txt"
        # Use a simpler JavaScript expression that maps over the files and joins with newlines
        input_files_expr = (
            "$(inputs['input-data'].map(function(f) { return f.path; }).join('\\n'))"
        )
        initial_workdir_listing.append(
            Dirent(
                entryname=input_files_manifest,
                entry=input_files_expr,
            )
        )

    init_workdir_requirement = InitialWorkDirRequirement(
        listing=initial_workdir_listing
    )

    # Build command - use the wrapper that handles prodConf conversion
    baseCommand = ["dirac-run-lbprodrun-app"]

    # Build arguments
    arguments = [prodconf_filename]

    # Add input files manifest argument if this step has input-data
    if "input-data" in [p.id for p in input_parameters]:
        input_files_manifest = f"inputFiles_{step_index + 1}.txt"
        arguments.extend(["--input-files", input_files_manifest])

    # Add replica catalog argument
    # The executor creates replica_catalog.json in the working directory
    arguments.extend(["--replica-catalog", "replica_catalog.json"])

    # Create the CommandLineTool
    requirements = [
        InlineJavascriptRequirement(),
        init_workdir_requirement,
    ]

    tool = CommandLineTool(
        id=f"{step_name}_tool",
        baseCommand=baseCommand,
        arguments=arguments,
        inputs=input_parameters,
        outputs=output_parameters,
        requirements=requirements,
    )

    return tool


def _generateProdConf(
    production: dict[str, Any], step: dict[str, Any], step_index: int
) -> dict[str, Any]:
    """Generate prodConf JSON for an analysis production step (similar to simulation.py)."""

    # Parse application info
    application = step.get("application", {})
    if isinstance(application, str):
        # Format: "AppName/version"
        parts = application.split("/")
        app_name = parts[0]
        app_version = parts[1] if len(parts) > 1 else "unknown"
        binary_tag = None
        nightly = None
    else:
        app_name = application.get("name", "unknown")
        app_version = application.get("version", "unknown")
        binary_tag = application.get("binary_tag")
        nightly = application.get("nightly")

    # Parse data packages
    data_pkgs = []
    for pkg in step.get("data_pkgs", []):
        if isinstance(pkg, str):
            data_pkgs.append(pkg)
        else:
            data_pkgs.append(f"{pkg.get('name')}.{pkg.get('version')}")

    # Build prodConf structure
    prod_conf = {
        "spec_version": 1,
        "application": {
            "name": app_name,
            "version": app_version,
            "data_pkgs": data_pkgs,
        },
        "options": {},
        "db_tags": {},
        "input": {},
        "output": {},
    }

    # Add binary tag if specified
    if binary_tag:
        prod_conf["application"]["binary_tag"] = binary_tag

    # Add nightly if specified
    if nightly:
        prod_conf["application"]["nightly"] = nightly

    # Build options configuration
    options = step.get("options", {})
    options_format = step.get("options_format")
    processing_pass = step.get("processing_pass")

    if isinstance(options, dict):
        # LbExec or other structured format (like merge steps with entrypoint)
        prod_conf["options"] = options
    elif isinstance(options, list):
        # List of option files
        prod_conf["options"]["files"] = options
        if processing_pass:
            prod_conf["options"]["processing_pass"] = processing_pass
        if options_format:
            prod_conf["options"]["format"] = options_format

    # DB Tags
    dbtags = step.get("dbtags", {})
    if dbtags:
        if dbtags.get("DDDB"):
            prod_conf["db_tags"]["dddb_tag"] = dbtags["DDDB"]
        if dbtags.get("CondDB"):
            prod_conf["db_tags"]["conddb_tag"] = dbtags["CondDB"]
        if dbtags.get("DQTag"):
            prod_conf["db_tags"]["dq_tag"] = dbtags["DQTag"]

    # Output types
    output_types = []
    for output in step.get("output", []):
        output_type = output.get("type")
        if output_type:
            output_types.append(
                output_type.lower()
            )  # TODO: check it's okay to lower here
    if output_types:
        prod_conf["output"]["types"] = output_types

    # Input configuration
    # For analysis productions, all input files will be processed fully
    prod_conf["input"]["first_event_number"] = 0
    prod_conf["input"]["n_of_events"] = -1

    return prod_conf


def _buildInputParameters(
    step: dict[str, Any], step_index: int, is_merge: bool, has_input_data: bool
) -> list[CommandInputParameter]:
    """Build input parameters for an analysis production step.

    Args:
        step: Step definition
        step_index: Global step index
        is_merge: Whether this is a merge step
        has_input_data: Whether this step receives input data (from previous step or transformation input)
    """

    input_parameters = []

    # Production ID and job ID
    input_parameters.append(
        CommandInputParameter(
            id="production-id",
            type_="int",
            # inputBinding=CommandLineBinding(prefix="--production-id"),
        )
    )

    input_parameters.append(
        CommandInputParameter(
            id="prod-job-id",
            type_="int",
            # inputBinding=CommandLineBinding(prefix="--prod-job-id"),
        )
    )

    # Output prefix (computed from production-id and prod-job-id)
    input_parameters.append(
        CommandInputParameter(
            id="output-prefix",
            type_="string",
            inputBinding=CommandLineBinding(prefix="--output-prefix"),
        )
    )

    # Input data files - all analysis production steps process input files
    # Either from previous step or from transformation input
    if has_input_data:
        input_parameters.append(
            CommandInputParameter(
                id="input-data",
                type_="File[]",
                # No inputBinding here - we'll handle it via InitialWorkDirRequirement
            )
        )

    return input_parameters


def _buildOutputParameters(step: dict[str, Any]) -> list[CommandOutputParameter]:
    """Build output parameters for an analysis production step."""

    output_parameters = []

    # Get output types from step
    output_types = step.get("output", [])

    # Main output data
    output_globs = []
    for output in output_types:
        output_type = output.get("type")
        if output_type:
            # Use case-insensitive glob pattern to handle any case variation
            output_globs.append(_make_case_insensitive_glob(output_type.lower()))

    if output_globs:
        output_parameters.append(
            CommandOutputParameter(
                id="output-data",
                type_="File[]",
                outputBinding=CommandOutputBinding(glob=output_globs),
            )
        )

    # Other outputs (logs, summaries, prodConf files)
    application = step.get("application", {})
    if isinstance(application, str):
        app_name = application.split("/")[0]
    else:
        app_name = application.get("name", "app")

    output_parameters.append(
        CommandOutputParameter(
            id="others",
            type_="File[]",
            outputBinding=CommandOutputBinding(
                glob=[
                    "prodConf*.json",
                    "prodConf*.py",
                    "summary*.xml",
                    "prmon*",
                    "*.log",
                ]
            ),
        )
    )

    return output_parameters


def _buildStepInputs(
    step: dict[str, Any],
    step_index: int,
    global_step_index: int,
    workflow_inputs: dict[str, WorkflowInputParameter],
    step_names: list[str],
    step_indices: list[int],
) -> list[WorkflowStepInput]:
    """Build workflow step inputs for an analysis production step.

    :param step: The step definition
    :param step_index: Local index of this step within the transformation (0-based)
    :param global_step_index: Global index of this step in the production (0-based)
    :param workflow_inputs: Transformation-level input parameters
    :param step_names: Names of all steps in this transformation (parallel to step_indices)
    :param step_indices: Global indices of all steps in this transformation
    """

    step_inputs = []

    # Pass through production-id and prod-job-id
    step_inputs.append(
        WorkflowStepInput(
            id="production-id",
            source="production-id",
        )
    )

    step_inputs.append(
        WorkflowStepInput(
            id="prod-job-id",
            source="prod-job-id",
        )
    )

    # Compute output-prefix from production-id and prod-job-id
    step_inputs.append(
        WorkflowStepInput(
            id="output-prefix",
            source=["production-id", "prod-job-id"],
            valueFrom=f'$(self[0].toString().padStart(8, "0"))_$(self[1].toString().padStart(8, "0"))_{global_step_index + 1}',
        )
    )

    # Handle input files
    # In analysis productions, steps either get input from a previous step or from transformation input
    step_input_refs = step.get("input", [])

    if step_index > 0:
        # Not the first step - get input from previous step in this transformation
        prev_step_name = step_names[step_index - 1]
        step_inputs.append(
            WorkflowStepInput(
                id="input-data",
                source=f"{prev_step_name}/output-data",
            )
        )
    elif "input-data" in workflow_inputs:
        # First step in a transformation that has input-data
        # Check if this step has explicit dependencies to a step in a different transformation
        if step_input_refs:
            source_step_idx = step_input_refs[0].get("step_idx")
            if source_step_idx not in step_indices:
                # Source is from a different transformation - use transformation input
                step_inputs.append(
                    WorkflowStepInput(
                        id="input-data",
                        source="input-data",
                    )
                )
        else:
            # No explicit dependencies - use transformation input-data
            step_inputs.append(
                WorkflowStepInput(
                    id="input-data",
                    source="input-data",
                )
            )

    return step_inputs


def _buildStepOutputs(step: dict[str, Any]) -> list[WorkflowStepOutput]:
    """Build workflow step outputs."""
    return [
        WorkflowStepOutput(id="output-data"),
        WorkflowStepOutput(id="others"),
    ]


def _getWorkflowStaticInputs(production: dict[str, Any]) -> dict[str, Any]:
    """Generate static input values for the workflow."""

    static_inputs = {
        "production-id": 12345,
        "prod-job-id": 6789,
    }

    # Note: input-data would typically come from a Bookkeeping query
    # For now, we'll leave it as a required input
    static_inputs["input-data"] = []

    return static_inputs


def _buildMainWorkflowStep(
    transform_name: str,
    transform_workflow: Workflow,
    transform: dict[str, Any],
    transform_index: int,
    workflow_inputs: dict[str, WorkflowInputParameter],
    transformation_names: list[str],
) -> WorkflowStep:
    """Build a step in the main workflow that references a transformation sub-workflow."""

    step_inputs = []

    # If this is not the first transformation, link to previous transformation's outputs
    if transform_index > 0:
        prev_transform_name = transformation_names[transform_index - 1]

        # Link output-data from previous transformation
        step_inputs.append(
            WorkflowStepInput(
                id="input-data",
                source=f"{prev_transform_name}/output-data",
            )
        )
    else:
        # First transformation gets input-data from workflow inputs
        step_inputs.append(
            WorkflowStepInput(
                id="input-data",
                source="input-data",
            )
        )

    # Always pass through common workflow inputs
    for input_id in ["production-id", "prod-job-id"]:
        if input_id in workflow_inputs:
            step_inputs.append(
                WorkflowStepInput(
                    id=input_id,
                    source=input_id,
                )
            )

    # Build step outputs
    step_outputs = [
        WorkflowStepOutput(id="output-data"),
        WorkflowStepOutput(id="others"),
    ]

    # Create the workflow step
    return WorkflowStep(
        id=transform_name,
        run=transform_workflow,
        in_=step_inputs,
        out=step_outputs,
    )


def _getTransformationOutputs(
    steps: list[dict[str, Any]], transform: dict[str, Any], step_names: list[str]
) -> list[WorkflowOutputParameter]:
    """Build outputs for a transformation sub-workflow.

    :param steps: All steps in the production
    :param transform: The transformation definition
    :param step_names: Names of steps in THIS transformation (indexed 0, 1, 2, ...)
    """

    # step_names is indexed by local step index (0, 1, 2, ...)
    # We should use all step names in this transformation
    output_sources = [f"{step_name}/output-data" for step_name in step_names]

    # Output data files from all steps in this transformation
    outputs = [
        WorkflowOutputParameter(
            id="output-data",
            type_="File[]",
            outputSource=output_sources,
            linkMerge="merge_flattened",
        ),
        WorkflowOutputParameter(
            id="others",
            type_={"type": "array", "items": ["File", "null"]},
            outputSource=[f"{step_name}/others" for step_name in step_names],
            linkMerge="merge_flattened",
        ),
    ]

    return outputs


def _getMainWorkflowOutputs(
    steps: list[dict[str, Any]],
    transforms: list[dict[str, Any]],
    transformation_names: list[str],
) -> list[WorkflowOutputParameter]:
    """Build outputs for the main workflow."""

    # Output data comes from the last transformation
    last_transform = (
        transformation_names[-1] if transformation_names else "transformation_1"
    )

    # Collect "others" outputs from ALL transformations to preserve logs/summaries
    all_other_output_sources = [
        f"{transform_name}/others" for transform_name in transformation_names
    ]

    return [
        WorkflowOutputParameter(
            id="output-data",
            label="Output Data",
            type_="File[]",
            outputSource=f"{last_transform}/output-data",
        ),
        WorkflowOutputParameter(
            id="others",
            label="Logs and summaries",
            type_="File[]",
            outputSource=all_other_output_sources,
            linkMerge="merge_flattened",
        ),
    ]
