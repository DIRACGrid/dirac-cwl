"""DIRAC CWL Proto - Common Workflow Language integration for DIRAC."""

from importlib.metadata import PackageNotFoundError, version

import typer

from dirac_cwl.job import app as job_app
from dirac_cwl.production import app as production_app
from dirac_cwl.transformation import app as transformation_app

try:
    __version__ = version("dirac-cwl")
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
