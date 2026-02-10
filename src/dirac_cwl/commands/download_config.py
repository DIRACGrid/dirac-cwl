"""Example pre-processing command that downloads configuration."""

import os

from dirac_cwl.commands import PreProcessCommand


class DownloadConfig(PreProcessCommand):
    """Example command that creates a file with named 'content.cfg'."""

    def execute(self, job_path, **kwargs):
        """Execute the configuration download.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        content = """\
This is an example
"""
        file_path = os.path.join(job_path, "content.cfg")
        with open(file_path, "w") as f:
            f.write(content)
