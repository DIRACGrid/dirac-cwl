"""DIRAC CWL Executor package.

This package provides custom executors for running CWL workflows with DIRAC-specific
functionality, including replica map management.
"""

from .executor import DiracExecutor, dirac_executor_factory

__all__ = ["DiracExecutor", "dirac_executor_factory"]
