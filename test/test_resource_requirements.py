"""Integration tests for CWL Resource Requirements validation."""

from typing import List, Optional

import pytest
from cwl_utils.parser.cwl_v1_2 import CommandLineTool, ResourceRequirement, Workflow, WorkflowStep

from dirac_cwl.submission_models import JobSubmissionModel, ProductionSubmissionModel, TransformationSubmissionModel

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------


def create_commandlinetool(
    requirements: Optional[list] = None,
    inputs: Optional[list] = None,
    outputs: Optional[list] = None,
) -> CommandLineTool:
    """Create a CommandLineTool with the given requirements, inputs, and outputs."""
    return CommandLineTool(
        requirements=requirements or [],
        inputs=inputs or [],
        outputs=outputs or [],
    )


def create_workflow(
    requirements: Optional[list] = None,
    steps: Optional[List[WorkflowStep]] = None,
    inputs: Optional[list] = None,
    outputs: Optional[list] = None,
) -> Workflow:
    """Create a Workflow with the given requirements, steps, inputs, and outputs."""
    return Workflow(
        requirements=requirements or [],
        steps=steps or [],
        inputs=inputs or [],
        outputs=outputs or [],
    )


def create_step(
    requirements: Optional[list] = None,
    run: Optional[CommandLineTool | Workflow] = None,
    in_: Optional[list] = None,
    out: Optional[list] = None,
) -> WorkflowStep:
    """Create a WorkflowStep with the given requirements, run, inputs, and outputs."""
    return WorkflowStep(
        requirements=requirements or [],
        run=run,
        in_=in_ or [],
        out=out or [],
    )


def assert_submission_fails(task):
    """Assert that submission fails with ValueError for Job and Transformation models with bad resource requirements.

    :param: CWL task to submit (Workflow, WorkflowStep, CommandLineTool, etc.)
    """
    with pytest.raises(ValueError):
        JobSubmissionModel(task=task)
    with pytest.raises(ValueError):
        TransformationSubmissionModel(task=task)


# -----------------------------------------------------------------------------
# Resource requirements tests
# -----------------------------------------------------------------------------
@pytest.mark.parametrize(
    "bad_min_max_reqs",
    [
        ResourceRequirement(coresMin=4, coresMax=2),
        ResourceRequirement(ramMin=2048, ramMax=1024),
        ResourceRequirement(tmpdirMin=1024, tmpdirMax=512),
        ResourceRequirement(outdirMin=512, outdirMax=256),
    ],
)
def test_bad_min_max_resource_reqs(bad_min_max_reqs):
    """Test invalid min/max resource requirements in CWL objects."""
    # CommandlineTool with bad minmax reqs
    clt = create_commandlinetool(requirements=[bad_min_max_reqs])
    assert_submission_fails(clt)

    # WorkflowStep.run with bad minmax reqs
    step_bad_run = create_step(run=clt)
    workflow = create_workflow(steps=[step_bad_run])
    assert_submission_fails(workflow)

    # WorkflowStep with bad minmax reqs
    clt = create_commandlinetool()
    step = create_step(run=clt, requirements=[bad_min_max_reqs])
    workflow = create_workflow(steps=[step])
    assert_submission_fails(workflow)

    # Workflow with bad minmax reqs
    workflow = create_workflow(requirements=[bad_min_max_reqs])
    assert_submission_fails(workflow)

    # NestedWorkflow with bad minmax reqs
    nest_workflow = create_workflow(requirements=[bad_min_max_reqs])
    step = create_step(run=nest_workflow)
    workflow = create_workflow(steps=[step])
    assert_submission_fails(workflow)

    # DeepNestedWorkflow with bad minmax reqs
    deep_workflow = create_workflow(requirements=[bad_min_max_reqs])
    deep_step = create_step(run=deep_workflow)
    nest_workflow = create_workflow(steps=[deep_step])
    step = create_step(run=nest_workflow)
    workflow = create_workflow(steps=[step])
    assert_submission_fails(workflow)


@pytest.mark.parametrize(
    ("wf_level_requirements", "higher_requirements"),
    [
        (ResourceRequirement(coresMax=2), ResourceRequirement(coresMin=4)),
        (ResourceRequirement(ramMax=512), ResourceRequirement(ramMin=1024)),
        (ResourceRequirement(tmpdirMax=512), ResourceRequirement(tmpdirMin=1024)),
        (ResourceRequirement(outdirMax=256), ResourceRequirement(outdirMin=512)),
    ],
)
def test_bad_wf_level_requirements(wf_level_requirements, higher_requirements):
    """Test global requirements conflicts."""
    # Workflow - WorkflowStep conflict
    step = create_step(requirements=[higher_requirements])
    workflow = create_workflow(requirements=[wf_level_requirements], steps=[step])
    assert_submission_fails(workflow)

    # Workflow - WorkflowStep.run conflict
    run = create_commandlinetool(requirements=[higher_requirements])
    step = create_step(run=run)
    workflow = create_workflow(requirements=[wf_level_requirements], steps=[step])
    assert_submission_fails(workflow)

    # Workflow - NestedWorkflow conflict
    nest_workflow = create_workflow(requirements=[higher_requirements])
    step = create_step(run=nest_workflow)
    workflow = create_workflow(requirements=[wf_level_requirements], steps=[step])
    assert_submission_fails(workflow)


@pytest.mark.parametrize(
    "requirements",
    [
        ResourceRequirement(coresMin=2, coresMax=4),
        ResourceRequirement(ramMin=1024, ramMax=2048),
        ResourceRequirement(tmpdirMin=512, tmpdirMax=1024),
        ResourceRequirement(outdirMin=256, outdirMax=512),
    ],
)
def test_production_requirements(requirements):
    """Test production case requirements."""
    # Production workflows can't have Workflow-level requirements
    workflow = create_workflow(requirements=[requirements])
    with pytest.raises(ValueError):
        ProductionSubmissionModel(task=workflow)

    # Production workflows can have step requirements
    step = create_step(requirements=[requirements])
    workflow = create_workflow(steps=[step])
    ProductionSubmissionModel(task=workflow)
