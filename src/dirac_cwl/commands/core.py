"""Core base classes for workflow processing commands."""

from abc import ABC, abstractmethod
from pathlib import Path


class CommandBase(ABC):
    """Base abstract class for pre/post-processing commands.

    New commands **MUST NOT** inherit this class. Instead they should inherit the interface classes
    :class:`dirac_cwl.commands.base.PreProcessCommand` and
    :class:`dirac_cwl.commands.base.PostProcessCommand`
    """

    @abstractmethod
    def execute(self, job_path: Path, **kwargs) -> None:
        """Execute the command in the given job path.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        :raises NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError("This method should be implemented by child class")


class PreProcessCommand(CommandBase):
    """Interface class for pre-processing commands.

    Every pre-processing command must inherit this class. Used for type validation.
    """


class PostProcessCommand(CommandBase):
    """Interface class for post-processing commands.

    Every post-processing command must inherit this class. Used for type validation.
    """
