"""Core base classes for workflow processing commands."""

import logging
import os
from abc import ABC, abstractmethod

from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.TransformationSystem.Client.FileReport import FileReport
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

from dirac_cwl.core.exceptions import WorkflowProcessingException

from .workflow_commons import WorkflowCommons

logger = logging.getLogger(__name__)


class CommandBase(ABC):
    """Base abstract class for pre/post-processing commands.

    New commands **MUST NOT** inherit this class. Instead they should inherit the interface classes
    :class:`dirac_cwl.commands.base.PreProcessCommand` and
    :class:`dirac_cwl.commands.base.PostProcessCommand`
    """

    request: Request = None
    failover_transfer: FailoverTransfer = None
    job_report: JobReport = None
    file_report: FileReport = None
    data_manager: DataManager = None
    bk_client: BookkeepingClient = None
    dsc: DataStoreClient = None

    def execute(self, job_path: os.PathLike, **kwargs) -> None:
        """Execute the command in the given job path.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        failed = False
        workflow_commons = None

        try:
            workflow_commons = WorkflowCommons.load(job_path)

            logger.info("WorkflowCommons:\n%s", workflow_commons)

            self._resolve_clients(workflow_commons)
            self._execute(job_path, workflow_commons, **kwargs)

        except WorkflowProcessingException:
            failed = True
            raise

        except Exception as e:
            logger.exception("Exception in %s", self.__class__.__name__, exc_info=e)

            failed = True
            if self.job_report:
                self.job_report.setApplicationStatus(repr(e))

            raise WorkflowProcessingException(e) from e

        finally:
            if workflow_commons:
                workflow_commons.save(job_path, request=self.request, dsc=self.dsc, failed=failed)

    def _resolve_clients(self, workflow_commons):
        """Initialize the required clients.

        JobReport is always needed, so when overriding, this needs to be called via super().
        """
        if not self.job_report:
            self.job_report = JobReport(workflow_commons.job_id)

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
