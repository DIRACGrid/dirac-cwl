"""Example post-processing command that groups output files."""

import glob
import os

from dirac_cwl_proto.commands import PostProcessCommand


class GroupOutputs(PostProcessCommand):
    """Example command that merges all of the outputs in a singular file."""

    def execute(self, job_path, **kwargs):
        """Execute the output file grouping.

        :param job_path: Path to the job working directory.
        :param kwargs: Additional keyword arguments.
        """
        grouped_outputs = "group.out"
        output_path = os.path.join(job_path, grouped_outputs)
        output_files = ["*.out", "*.txt"]

        with open(output_path, "w", encoding="utf-8") as f_in:
            for file_type in output_files:
                extension = f"{job_path}/{file_type}"
                for file in glob.glob(extension):
                    if file == output_path:
                        continue

                    with open(file, "r", encoding="utf-8") as f_out:
                        f_in.write(f"############ {file}\n")
                        f_in.writelines(f_out.readlines())
                        f_in.write("\n")
