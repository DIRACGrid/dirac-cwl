from abc import ABC, abstractmethod
from pydantic import BaseModel, ConfigDict
from cwl_utils.parser.cwl_v1_2 import Workflow, CommandLineTool, WorkflowStep
from typing import ClassVar

class RequirementValidator(BaseModel, ABC):
    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

    requirement_class: ClassVar[str]
    cwl_object: Workflow | CommandLineTool | WorkflowStep # TODO: might need to add ExpressionTool later? and NestedWorkflow?

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

    @abstractmethod
    def validate_requirement(self, requirement, context: str = "", global_requirement = None):
        """
        Validate a requirement, specific for each requirement class.

        :param requirement: The current requirement to validate.
        :param context: A context string describing the validation context. Ex: "Step requirement", "Global requirement", etc.
        :param global_requirement: The global Workflow requirement, if any.
        """
        pass

    def validate(self):
        """
        Validate requirements in Workflow, WorkflowStep and WorkflowStep.run.
        # TODO: might need to add ExpressionTool later and also, find a way to validate NestedWorkflow?
        """
        global_requirement = self.get_requirement(self.cwl_object)

        # Validate Workflow/CommandLineTool global requirements.
        if global_requirement:
            self.validate_requirement(global_requirement, context="global requirements")

        # Validate WorkflowStep requirements, if any.
        if self.cwl_object.class_ != 'CommandLineTool' and self.cwl_object.steps:
            for step in self.cwl_object.steps:
                step_requirement = self.get_requirement(step)
                if step_requirement:
                    self.validate_requirement(step_requirement,
                                              context="step requirements",
                                              global_requirement=global_requirement)
                # Validate WorkflowStep.run, if any.
                if step.run:
                    step_run_requirement = self.get_requirement(step.run)
                    if step_run_requirement:
                        self.validate_requirement(step_run_requirement,
                                                  context="step run requirements",
                                                  global_requirement=global_requirement)


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
        :param context: A context string describing the validation context. Ex: "Step requirement", "Global requirement", etc.
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
            :param context: A context string describing the validation context. Ex: "Step requirement", "Global requirement", etc.
        """
        if min_value and global_max_value and min_value > global_max_value:
            raise ValueError(f"{resource}Min is higher than global {resource}Max in {context}")

    def validate_requirement(self, requirement, context: str = "", global_requirement = None):
        """
        Validate a ResourceRequirement.
        Verify:
         - that resourceMin is not higher than resourceMax (CommandLineTool, Workflow, WorkflowStep, WorkflowStep.run)
         - that resourceMin (WorkflowStep, WorkflowStep.run) is not higher than global (Workflow) resourceMax.

        :param requirement: The current ResourceRequirement to validate.
        :param context: A context string describing the validation context. Ex: "Step requirement", "Global requirement", etc.
        :param global_requirement: The global Workflow requirement, if any.
        """
        self.validate_minmax(requirement.ramMin, requirement.ramMax, "ram", context)
        self.validate_minmax(requirement.coresMin, requirement.coresMax, "cores", context)

        if global_requirement:
            # Only WorkflowStep and WorkflowStep.run cases
            self.validate_conflict(requirement.ramMin, global_requirement.ramMax, "ram", context)
            self.validate_conflict(requirement.coresMin, global_requirement.coresMax, "cores", context)