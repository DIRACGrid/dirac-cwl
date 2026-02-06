<p align="center">
  <img alt="Dirac CWL Logo" src="public/CWLDiracX.png" width="300" >
</p>

# Dirac CWL Prototype
![Workflow tests](https://github.com/DIRACGrid/dirac-cwl/actions/workflows/main.yml/badge.svg?branch=main)
![Schema Generation](https://github.com/DIRACGrid/dirac-cwl/actions/workflows/generate-schemas.yml/badge.svg?branch=main) [![Conda Recipe](https://img.shields.io/badge/recipe-dirac--cwl-green.svg)](https://anaconda.org/conda-forge/dirac-cwl) 

This Python prototype introduces a command-line interface (CLI) designed for the end-to-end execution of Common Workflow Language (CWL) workflows at different scales. It enables users to locally test CWL workflows, and then run them as jobs, transformations and/or productions.

## Prototype Workflow

### Local testing

Initially, the user tests the CWL workflow locally using `cwltool`. This step involves validating the workflow's structure and ensuring that it executes correctly with the provided inputs.

  > - CWL task: workflow structure
  > - inputs of the task

Once the workflow passes local testing, the user can choose from 3 options for submission depending on the requirements.

### Submission methods

1. **Submission as Dirac Jobs**: For simple workflows with a limited number of inputs, CWL tasks can be submitted as individual jobs. In this context, they are run locally as if they were run on distributed computing resources. Additionally, users can submit the same workflow with different sets of inputs in a single request, generating multiple jobs at once.

  > - CWL task
  > - [inputs1, inputs2, ...]
  > - Dirac description (site, priority):  Dirac-specific attributes related to scheduling
  > - Metadata (job type): Dirac-specific attributes related to scheduling + execution

2. **Submission as Dirac Transformation**: For workflows requiring continuous, real-time input data or large-scale execution, CWL tasks can be submitted as transformations. As new input data becomes available, jobs are automatically generated and executed as jobs. This method is ideal for ongoing data processing and scalable operations.

  > - CWL task (inputs already described within it)
  > - Dirac description (site, priority)
  > - Metadata (job type, group size, query parameters)

3. **Submission as Dirac Productions**: For complex workflows that require multiple steps with different requirements, CWL tasks can be submitted as productions. This method allows the workflow to be split into multiple transformations, with each transformation handling a distinct step in the process. Each transformation can manage one or more jobs, enabling large-scale, multi-step execution.

  > - CWL task (inputs already described within it)
  > - Step Metadata (per step):
  >   - Dirac description (site, priority)
  >   - Metadata (job type, group size, query parameters)

## Installation (with Pixi)

This project uses [Pixi](https://pixi.sh) to manage the development environment and tasks.

1) Install Pixi (see official docs for your platform)

2) Create and populate the environment

```bash
pixi install
```

3) Enter the environment (optional)

```bash
pixi shell
```

That’s it. You can now run commands either inside `pixi shell` or by prefixing with `pixi run`.

## Usage

Inside the Pixi environment:

```bash
# Either inside a shell
pixi shell

# Submit
dirac-cwl job submit <workflow_path> [--parameter-path <input_path>] [--metadata-path <metadata_path>]

dirac-cwl transformation submit <workflow_path> [--metadata-path <metadata_path>]

dirac-cwl production submit <workflow_path> [--steps-metadata-path <steps_metadata_path>]
```

Or prefix individual commands:

```bash
pixi run dirac-cwl job submit <workflow_path> --parameter-path <input_path>
```

Common tasks are defined in `pyproject.toml` and can be run with Pixi:

```bash
# Run tests
pixi run test

# Lint (mypy)
pixi run lint
```

## Using cwltool directly

To use the workflows and inputs directly with `cwltool`, you need to add the `modules` directory to the `$PATH`:

```bash
export PATH=$PATH:</path/to/dirac-cwl/src/dirac_cwl/modules>
cwltool <workflow_path> <inputs>
```

## Contribute

### Add a workflow

To add a new workflow to the project, follow these steps:

- Create a new directory under `workflows` (e.g. `workflows/helloworld`)
- Add one or more variants of a workflow under different directory (e.g. `helloworld/helloworld_basic/description.cwl` and `helloworld/helloworld_with_inputs/description.cwl`)
- In a `type_dependencies` subdirectory, add the required files to submit a job/transformation/production from a given variant.

Directory Structure Example:

```
workflows/
└── my_new_workflow/
    |
    ├── my_new_workflow_complete/
    |   └── description.cwl
    ├── my_new_workflow_step1/
    |   └── description.cwl
    ├── my_new_workflow_step2/
    |   └── description.cwl
    |
    └── type_dependencies/
        ├── production/
        |   └── steps_metadata.yaml
        ├── transformation/
        |   └── metadata.yaml
        └── job/
            ├── inputs1.yaml
            └── inputs2.yaml
```

### Add a Pre/Post-processing commamd and a Job type

#### Add a Pre/Post-Command

A pre/post-processing command allows the execution of code before and after the workflow.

The commands should be stored at the `src/dirac_cwl/commands/` directory

To add a new pre/post-processing command to the project, follow these steps:

- Create a class that inherits `PreProcessCommand` if it's going to be executed before the workflow or `PostProcessCommand` if it's going to be executed after the workflow. In the rare case that the command can be executed in both stages, it should inherit both classes. These classes are located at `src/dirac_cwl/commands/core.py`.

- Implement the `execute` function with the actions it's expected to do. This function recieves the `job path` as a `string` and the dictionary of keyworded arguments `**kwargs`. This function can raise exceptions if it needs to.

#### Add a Job Type

Job types in `dirac_cwl` have the name of "plugins". These plugins are created from the hints defined in a cwl file.

The Job type should be stored at the `src/dirac_cwl/execution_hooks/plugins/` directory and should appear in the `__all__` list of the `__init__.py` file.

To add a new Job type to the project, follow these steps:

- Create a class that inherits `ExecutionHooksBasePlugin` from `src/dirac_cwl/execution_hooks/core.py`.

- Import the pre-processing and post-processing commands that this Job type is going to execute.

- Inside the `__init__` function, set the `preprocess_commands` and `postprocess_commands` lists with the commands that each step should execute. Be specially careful in the order, the commands will be executed in the same order they were specified in the lists.

In the end, it should look something like this:

```python
class JobTypeExample(ExecutionHooksBasePlugin):
  def __init__(self, **data):
    super().__init__(**data)

    # ...
    self.preprocess_commands = [PreProcessCmd1, PreProcessCmd2, PreProcessCmd3]
    self.postprocess_commands = [PostProcessCmd1, PostProcessCmd2, PostProcessCmd3]
    # ...
```

In the previous example, `PreProcessCmd1` will be executed before `PreProcessCmd2`, and this will be executed before `PreProcessCmd3`.

- Finally, to be able to discover this plugin from the registry, it has to appear in `pyproject.toml` entrypoints at the group `dirac_cwl.execution_hooks`. The previous example would look like:

```toml
[project.entry-points."dirac_cwl.execution_hooks"]
# ...
JobTypeExample = "dirac_cwl.execution_hooks.plugins:JobTypeExample"
# ...
```

### Add a module

If your workflow requires calling a script, you can add this script as a module. Follow these steps to properly integrate the module:

- Add the script: Place your script in the `src/dirac_cwl/modules` directory.
- Update `pyproject.toml`: Add the script to the `pyproject.toml` file to create a command-line interface (CLI) command.
- Reinstall the package: Run `pixi run pip install .` to reinstall the package and make the new script available as a command.
- Usage in CWL Workflow: Reference the command in your `description.cwl` file.

**Example**

Let’s say you have a script named `generic_command.py` located at `src/dirac_cwl/modules/generic_command.py`. Here's how you can integrate it:

- `generic_command.py` Example Script:

```python
#!/usr/bin/env python3
import typer
from rich.console import Console

app = typer.Typer()
console = Console()

@app.command()
def run_example():
    console.print("This is an example command.")

if __name__ == "__main__":
    app()
```

- Update `pyproject.toml`:

```toml
[project.scripts]
generic-command = "dirac_cwl.modules.generic_command:app"
```

- Reinstall the package with:

```bash
pixi run pip install .
```

- Reference in `description.cwl`:

```yaml
baseCommand: [generic-command]
```

### Test your changes

- Run tests via Pixi:

```bash
pixi run test
```

- Or directly:

```bash
pixi run pytest test/test_workflows.py -v
```
