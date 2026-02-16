"""Integration tests for CWL Resource Requirements validation."""

import pytest
from cwl_utils.parser.cwl_v1_2 import (
    CommandLineTool,
    ResourceRequirement,
    Workflow,
    WorkflowStep,
)

from dirac_cwl.submission_models import JobSubmissionModel, ProductionSubmissionModel, TransformationSubmissionModel


# Helper functions
def create_commandlinetool(requirements=None, inputs=None, outputs=None):
    """Create a CommandLineTool with the given requirements, inputs, and outputs."""
    return CommandLineTool(
        requirements=requirements or [],
        inputs=inputs or [],
        outputs=outputs or [],
    )


def create_workflow(requirements=None, steps=None, inputs=None, outputs=None):
    """Create a Workflow with the given requirements, steps, inputs, and outputs."""
    return Workflow(
        requirements=requirements or [],
        steps=steps or [],
        inputs=inputs or [],
        outputs=outputs or [],
    )


def create_step(requirements=None, run=None, in_=None, out=None):
    """Create a WorkflowStep with the given requirements, run, inputs, and outputs."""
    return WorkflowStep(
        requirements=requirements or [],
        run=run,
        in_=in_ or [],
        out=out or [],
    )


def submit_task(task):
    """Submit failing tasks."""
    with pytest.raises(ValueError):
        JobSubmissionModel(task=task)
        TransformationSubmissionModel(task=task)


@pytest.mark.parametrize(
    "bad_min_max_reqs",
    [
        # cores
        ResourceRequirement(coresMin=4, coresMax=2),
        # ram
        ResourceRequirement(ramMin=2048, ramMax=1024),
        # tmpdir
        ResourceRequirement(tmpdirMin=1024, tmpdirMax=512),
        # outdir
        ResourceRequirement(outdirMin=512, outdirMax=256),
    ],
)
def test_bad_min_max_resource_reqs(bad_min_max_reqs):
    """Test invalid min/max resource requirements in CWL objects."""
    # CommandlineTool with bad minmax reqs
    clt = create_commandlinetool(requirements=[bad_min_max_reqs])
    submit_task(clt)

    # WorkflowStep.run with bad minmax reqs
    step_bad_run = create_step(run=clt)
    workflow = create_workflow(steps=[step_bad_run])
    submit_task(workflow)

    # WorkflowStep with bad minmax reqs
    clt = create_commandlinetool()
    step = create_step(run=clt, requirements=[bad_min_max_reqs])
    workflow = create_workflow(steps=[step])
    submit_task(workflow)

    # Workflow with bad minmax reqs
    workflow = create_workflow(requirements=[bad_min_max_reqs])
    submit_task(workflow)

    # NestedWorkflow with bad minmax reqs
    nest_workflow = create_workflow(requirements=[bad_min_max_reqs])
    step = create_step(run=nest_workflow)
    workflow = create_workflow(steps=[step])
    submit_task(workflow)

    # DeepNestedWorkflow with bad minmax reqs
    deep_workflow = create_workflow(requirements=[bad_min_max_reqs])
    deep_step = create_step(run=deep_workflow)
    nest_workflow = create_workflow(steps=[deep_step])
    step = create_step(run=nest_workflow)
    workflow = create_workflow(steps=[step])
    submit_task(workflow)


@pytest.mark.parametrize(
    ("global_requirements", "higher_requirements"),
    [
        # cores
        (
            ResourceRequirement(coresMax=2),
            ResourceRequirement(coresMin=4),
        ),
        # ram
        (
            ResourceRequirement(ramMax=512),
            ResourceRequirement(ramMin=1024),
        ),
        # tmpdir
        (
            ResourceRequirement(tmpdirMax=512),
            ResourceRequirement(tmpdirMin=1024),
        ),
        # outdir
        (ResourceRequirement(outdirMax=256), ResourceRequirement(outdirMin=512)),
    ],
)
def test_bad_global_requirements(global_requirements, higher_requirements):
    """Test global requirements conflicts."""
    # Workflow - WorkflowStep conflict
    step = create_step(requirements=[higher_requirements])
    workflow = create_workflow(requirements=[global_requirements], steps=[step])
    submit_task(workflow)

    # Workflow - WorkflowStep.run conflict
    run = create_commandlinetool(requirements=[higher_requirements])
    step = create_step(run=run)
    workflow = create_workflow(requirements=[global_requirements], steps=[step])
    submit_task(workflow)

    # Workflow - NestedWorkflow conflict
    nest_workflow = create_workflow(requirements=[higher_requirements])
    step = create_step(run=nest_workflow)
    workflow = create_workflow(requirements=[global_requirements], steps=[step])
    submit_task(workflow)


@pytest.mark.parametrize(
    "requirements",
    [
        # cores
        ResourceRequirement(coresMin=2, coresMax=4),
        # ram
        ResourceRequirement(ramMin=1024, ramMax=2048),
        # tmpdir
        ResourceRequirement(tmpdirMin=512, tmpdirMax=1024),
        # outdir
        ResourceRequirement(outdirMin=256, outdirMax=512),
    ],
)
def test_production_requirements(requirements):
    """Test production case requirements."""
    # Production workflows can't have global requirements
    workflow = create_workflow(requirements=[requirements])
    with pytest.raises(ValueError):
        ProductionSubmissionModel(task=workflow)

    # Production workflows can have step requirements
    step = create_step(requirements=[requirements])
    workflow = create_workflow(steps=[step])
    ProductionSubmissionModel(task=workflow)
