from abc import ABC, abstractmethod
from typing import ClassVar

from cwl_utils.parser.cwl_v1_2 import CommandLineTool, Workflow, WorkflowStep
from pydantic import BaseModel, ConfigDict


class RequirementValidator(BaseModel, ABC):
    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

    requirement_class: ClassVar[str]
    cwl_object: Workflow | CommandLineTool | WorkflowStep

    def get_requirement(self, cwl_object: Workflow | CommandLineTool | WorkflowStep):
        """
        Extract the requirement from the current cwl_object,
        based on the requirement class we want.

        :param cwl_object: The cwl_object to extract the requirement from.
        :return: The requirement object, or None if not found.
        """
        requirements = getattr(cwl_object, "requirements", []) or []
        for requirement in requirements:
            if requirement.class_ == self.requirement_class:
                return requirement
        return None

    # TODO : get all nested workflows of a workflow,
    # for each nested workflow, validate its requirements
    def get_nested_workflows(self):
        pass

    @abstractmethod
    def validate_requirement(
        self, requirement, context: str = "", global_requirement=None
    ):
        """
        Validate a requirement, specific for each requirement class.

        :param requirement: The current requirement to validate.
        :param context: A context string describing the validation context.
        Ex: "Step requirement", "Global requirement", etc.
        :param global_requirement: The global Workflow requirement, if any.
        """
        pass

    @abstractmethod
    def validate_production_requirement(self, requirement, is_global: bool = False):
        """
        Validate a production workflow requirement, specific for each requirement class.
        This method also depends on the production workflow rules.

        :param requirement: The current requirement to validate.
        :param is_global: True if it's a global requirement, False otherwise.
        """
        pass

    def validate_requirements(self, production: bool = False) -> None:
        """
        Validate all requirements in Workflow, WorkflowStep and WorkflowStep.run.

        :param production: True if the requirements are for a production workflow, False otherwise.
        Maybe we could add a transformation parameter later.
        """
        global_requirement = self.get_requirement(self.cwl_object)

        # Validate Workflow/CommandLineTool global requirements.
        if global_requirement:
            if production:
                self.validate_production_requirement(global_requirement, is_global=True)
            self.validate_requirement(global_requirement, context="global requirements")

        # Validate WorkflowStep requirements, if any.
        if self.cwl_object.class_ != "CommandLineTool" and self.cwl_object.steps:
            self.validate_steps_requirements(global_requirement, production)

    def validate_steps_requirements(self, global_requirement, production: bool = False):
        """
        Validate steps requirements in WorkflowStep and WorkflowStep.run.

        :param global_requirement: The global Workflow requirement, if any.
        :param production: True if the requirements are for a production workflow, False otherwise.
        """
        # Validate WorkflowStep requirements, if any.
        for step in self.cwl_object.steps:
            step_requirement = self.get_requirement(step)
            if step_requirement:
                if production:
                    self.validate_production_requirement(step_requirement)
                self.validate_requirement(
                    step_requirement,
                    context="step requirements",
                    global_requirement=global_requirement,
                )
            # Validate WorkflowStep.run, if any.
            if step.run:
                step_run_requirement = self.get_requirement(step.run)
                if step_run_requirement:
                    if production:
                        self.validate_production_requirement(step_run_requirement)
                    self.validate_requirement(
                        step_run_requirement,
                        context="step run requirements",
                        global_requirement=global_requirement,
                    )


class RequirementError(Exception):
    def __init__(self, message):
        super().__init__(message)


class ResourceRequirementValidator(RequirementValidator):
    requirement_class = "ResourceRequirement"

    @staticmethod
    def validate_minmax(min_value, max_value, resource, context):
        """
        Check if the resource min_value is higher than the resource max_value.
        If so, raise a ValueError.

        :param min_value: The current resource min_value.
        :param max_value: The current resource max_value.
        :param resource: The resource name.
        :param context: A context string describing the validation context.
        Ex: "Step requirement", "Global requirement", etc.
        """
        if min_value and max_value and min_value > max_value:
            raise ValueError(f"{resource}Min is higher than {resource}Max in {context}")

    @staticmethod
    def validate_conflict(min_value, global_max_value, resource, context):
        """
        Check if the resource min_value is higher than the global resource max_value.
        If so, raise a ValueError.

        :param min_value: The current resource min_value.
        :param global_max_value: The global resource max_value.
        :param resource: The resource name.
        :param context: A context string describing the validation context.
        Ex: "Step requirement", "Global requirement", etc.
        """
        if min_value and global_max_value and min_value > global_max_value:
            raise ValueError(
                f"{resource}Min is higher than global {resource}Max in {context}"
            )

    def validate_production_requirement(self, requirement, is_global: bool = False):
        """
        Raise an error if there's a global ResourceRequirement in a production workflow.
        Otherwise, add some logic for WorkflowSteps, CommandLineTools and WorkflowStep.run in production workflows.

        :param requirement: The current requirement to validate.
        :param is_global: True if there's a global ResourceRequirement, False otherwise.
        """
        if is_global:
            raise RequirementError(
                "ResourceRequirement is invalid: global ResourceRequirement is not allowed in production workflows"
            )
        # else ...

    def validate_requirement(
        self, requirement, context: str = "", global_requirement=None
    ):
        """
        Validate a ResourceRequirement.
        Verify:
         - that resourceMin is not higher than resourceMax (CommandLineTool, Workflow, WorkflowStep, WorkflowStep.run)
         - that resourceMin (WorkflowStep, WorkflowStep.run) is not higher than global (Workflow) resourceMax.

        :param requirement: The current ResourceRequirement to validate.
        :param context: A context string describing the validation context.
        Ex: "Step requirement", "Global requirement", etc.
        :param global_requirement: The global Workflow requirement, if any.
        """
        try:
            self.validate_minmax(requirement.ramMin, requirement.ramMax, "ram", context)
            self.validate_minmax(
                requirement.coresMin, requirement.coresMax, "cores", context
            )

            if global_requirement:
                # Only WorkflowStep and WorkflowStep.run cases
                self.validate_conflict(
                    requirement.ramMin, global_requirement.ramMax, "ram", context
                )
                self.validate_conflict(
                    requirement.coresMin, global_requirement.coresMax, "cores", context
                )
        except ValueError as ex:
            raise RequirementError(f"ResourceRequirement is invalid: {ex}") from ex
