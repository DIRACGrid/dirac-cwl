"""Core base classes for workflow processing commands."""

import logging
import os
from abc import ABC, abstractmethod

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .workflow_commons import WorkflowCommons

logger = logging.getLogger(__name__)


class CommandBase(ABC):
    """Base abstract class for pre/post-processing commands.

    New commands **MUST NOT** inherit this class. Instead they should inherit the interface classes
    :class:`dirac_cwl.commands.base.PreProcessCommand` and
    :class:`dirac_cwl.commands.base.PostProcessCommand`
    """

    def execute(self, job_path: os.PathLike, **kwargs) -> None:
        """Execute the command in the given job path.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        failed = False
        workflow_commons = None
        try:
            workflow_commons = WorkflowCommons.load(job_path)

            self._execute(job_path, workflow_commons, **kwargs)

        except WorkflowProcessingException:
            failed = True
            raise

        except Exception as e:
            logger.exception("Exception in %s", self.__class__.__name__, exc_info=e)

            failed = True
            if workflow_commons:
                workflow_commons.job_report.setApplicationStatus(repr(e))

            raise WorkflowProcessingException(e) from e

        finally:
            if workflow_commons:
                workflow_commons.save(job_path, failed=failed)

    @abstractmethod
    def _execute(self, job_path: os.PathLike, workflow_commons: WorkflowCommons, **kwargs) -> None:
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
