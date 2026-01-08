"""DIRAC CWL Proto - Common Workflow Language integration for DIRAC."""

import logging
from importlib.metadata import PackageNotFoundError, version

import typer

from dirac_cwl_proto.job import app as job_app
from dirac_cwl_proto.production import app as production_app
from dirac_cwl_proto.transformation import app as transformation_app

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")

try:
    __version__ = version("dirac-cwl-proto")
except PackageNotFoundError:
    # package is not installed
    pass

app = typer.Typer()

# Add sub-apps
app.add_typer(production_app, name="production")
app.add_typer(transformation_app, name="transformation")
app.add_typer(job_app, name="job")

if __name__ == "__main__":
    app()
