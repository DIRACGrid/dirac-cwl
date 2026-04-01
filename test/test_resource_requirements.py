"""Integration tests for CWL Resource Requirements validation."""

from typing import Optional

import pytest
from cwl_utils.parser.cwl_v1_2 import CommandLineTool, ExpressionTool, ResourceRequirement, Workflow, WorkflowStep

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
    steps: Optional[list[WorkflowStep]] = None,
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


def create_expressiontool(
    requirements: Optional[list] = None,
    inputs: Optional[list] = None,
    outputs: Optional[list] = None,
) -> ExpressionTool:
    """Create an ExpressionTool with the given requirements, inputs, and outputs."""
    return ExpressionTool(
        expression="",
        requirements=requirements or [],
        inputs=inputs or [],
        outputs=outputs or [],
    )


def assert_submission_fails(task):
    """Assert that submission fails with ValueError for Job and Transformation models with bad resource requirements.

    :param: CWL task to submit (Workflow, WorkflowStep, CommandLineTool, etc.)
    """
    with pytest.raises(ValueError):
        JobSubmissionModel(task=task)
    with pytest.raises(ValueError):
        TransformationSubmissionModel(task=task)
    with pytest.raises(ValueError):
        ProductionSubmissionModel(task=task)


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

    # ExpressionTool with bad minmax reqs
    expression_tool = create_expressiontool(requirements=[bad_min_max_reqs])
    assert_submission_fails(expression_tool)

    # WorkflowStep.run with bad minmax reqs
    step_bad_run = create_step(run=clt)
    workflow = create_workflow(steps=[step_bad_run])
    assert_submission_fails(workflow)

    step_bad_run = create_step(run=expression_tool)
    workflow = create_workflow(steps=[step_bad_run])
    assert_submission_fails(workflow)

    # WorkflowStep with bad minmax reqs
    clt = create_commandlinetool()
    step = create_step(run=clt, requirements=[bad_min_max_reqs])
    workflow = create_workflow(steps=[step])
    assert_submission_fails(workflow)

    expression_tool = create_commandlinetool()
    step = create_step(run=expression_tool, requirements=[bad_min_max_reqs])
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
