"""CLI tool for running CWL workflows with DIRAC executor.

This command-line tool runs CWL workflows using the DiracExecutor, which handles
replica map management for input and output files.
"""

import logging
import sys
from datetime import datetime, timezone
from importlib.metadata import version as get_version
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def _get_package_version(package: str) -> str:
    """Get version of a package, returning 'unknown' if not installed."""
    try:
        return get_version(package)
    except Exception:
        return "unknown"


# Create Typer app with context settings to allow extra arguments
app = typer.Typer(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})


# Configure logging to use UTC
def configure_utc_logging():
    """Configure logging to use UTC timestamps."""
    logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()


def version_callback(value: bool):
    """Handle --version flag."""
    if value:
        console.print("[cyan]dirac-cwl executor[/cyan]")
        console.print(f"Version: [green]{_get_package_version('dirac-cwl')}[/green]")
        console.print()
        console.print("[dim]Dependencies:[/dim]")
        console.print(f"  cwltool: [blue]{_get_package_version('cwltool')}[/blue]")
        console.print(f"  diracx-core: [blue]{_get_package_version('diracx-core')}[/blue]")
        console.print(f"  diracx-client: [blue]{_get_package_version('diracx-client')}[/blue]")
        console.print(f"  DIRAC: [blue]{_get_package_version('DIRAC')}[/blue]")
        raise typer.Exit()


def print_workflow_visualization(workflow_path: Path):
    """Print a nice visualization of the workflow structure with graph representation."""
    import yaml

    try:
        with open(workflow_path, "r") as f:
            cwl = yaml.safe_load(f)

        console.print()
        console.print(
            Panel.fit(
                f"[bold cyan]Workflow Visualization[/bold cyan]\n[dim]{workflow_path.name}[/dim]",
                border_style="cyan",
            )
        )

        # Show basic info
        cwl_version = cwl.get("cwlVersion", "Unknown")
        doc = cwl.get("doc", cwl.get("label", ""))

        info_table = Table(show_header=False, box=None, padding=(0, 2))
        info_table.add_column("Key", style="bold cyan")
        info_table.add_column("Value")

        info_table.add_row("CWL Version:", cwl_version)
        if doc:
            info_table.add_row("Description:", doc)

        console.print(info_table)
        console.print()

        # Show inputs (handle both dict and list formats)
        inputs = cwl.get("inputs", {})
        if inputs:
            console.print("[bold green]📥 INPUTS:[/bold green]")
            if isinstance(inputs, dict):
                for name, spec in inputs.items():
                    input_type = spec.get("type", "unknown") if isinstance(spec, dict) else spec
                    label = spec.get("label", "") if isinstance(spec, dict) else ""
                    label_str = f" [dim]({label})[/dim]" if label else ""
                    console.print(f"  • [cyan]{name}[/cyan]: {input_type}{label_str}")
            elif isinstance(inputs, list):
                for inp in inputs:
                    if isinstance(inp, dict):
                        name = inp.get("id", "unknown")
                        input_type = inp.get("type", "unknown")
                        label = inp.get("label", inp.get("doc", ""))
                        label_str = f" [dim]({label})[/dim]" if label else ""
                        console.print(f"  • [cyan]{name}[/cyan]: {input_type}{label_str}")
            console.print()

        # Build and show graph representation (handle both dict and list formats)
        steps = cwl.get("steps", {})
        outputs = cwl.get("outputs", {})

        if steps:
            console.print("[bold yellow]🔀 WORKFLOW GRAPH:[/bold yellow]")
            console.print()

            # Build dependency graph (handle both dict and list formats)
            if isinstance(steps, dict):
                step_list = list(steps.items())
            elif isinstance(steps, list):
                # Convert list format to (name, spec) tuples
                step_list = [(s.get("id", f"step_{i}"), s) for i, s in enumerate(steps)]
            else:
                step_list = []

            # Print graph representation
            for i, (step_name, step_spec) in enumerate(step_list):
                if isinstance(step_spec, dict):
                    is_last = i == len(step_list) - 1

                    # Print step box
                    step_prefix = "└──" if is_last else "├──"
                    step_label = step_spec.get("label", step_name)
                    console.print(f"{step_prefix} [bold cyan]{step_name}[/bold cyan] [dim]({step_label})[/dim]")

                    # Indentation for details
                    detail_prefix = "    " if is_last else "│   "

                    # Show inputs with arrows (handle both dict and list formats)
                    step_in = step_spec.get("in", {})
                    if step_in:
                        if isinstance(step_in, dict):
                            for in_name, in_source in step_in.items():
                                source = (
                                    in_source
                                    if isinstance(in_source, str)
                                    else (in_source.get("source", "?") if isinstance(in_source, dict) else "?")
                                )
                                console.print(f"{detail_prefix}  [green]⬅[/green] {in_name} [dim]←[/dim] {source}")
                        elif isinstance(step_in, list):
                            for inp in step_in:
                                if isinstance(inp, dict):
                                    in_name = inp.get("id", "?")
                                    source = inp.get("source", "?")
                                    console.print(f"{detail_prefix}  [green]⬅[/green] {in_name} [dim]←[/dim] {source}")

                    # Show outputs with arrows (handle both dict and list formats)
                    step_out = step_spec.get("out", [])
                    if step_out:
                        if isinstance(step_out, list):
                            for out in step_out:
                                out_name = out.get("id", out) if isinstance(out, dict) else out
                                console.print(f"{detail_prefix}  [yellow]➡[/yellow] {out_name}")

                    if not is_last:
                        console.print("│")

            console.print()

        # Show final outputs (handle both dict and list formats)
        if outputs:
            console.print("[bold magenta]📤 FINAL OUTPUTS:[/bold magenta]")
            if isinstance(outputs, dict):
                for name, spec in outputs.items():
                    output_type = spec.get("type", "unknown") if isinstance(spec, dict) else spec
                    source = spec.get("outputSource", "") if isinstance(spec, dict) else ""
                    source_str = f" [dim]← {source}[/dim]" if source else ""
                    console.print(f"  • [cyan]{name}[/cyan]: {output_type}{source_str}")
            elif isinstance(outputs, list):
                for out in outputs:
                    if isinstance(out, dict):
                        name = out.get("id", "unknown")
                        output_type = out.get("type", "unknown")
                        source = out.get("outputSource", "")
                        label = out.get("label", "")
                        label_str = f" [dim]({label})[/dim]" if label else ""
                        source_str = f" [dim]← {source}[/dim]" if source else ""
                        console.print(f"  • [cyan]{name}[/cyan]: {output_type}{label_str}{source_str}")
            console.print()

        # Show hints
        hints = cwl.get("hints", [])
        if hints:
            console.print("[bold blue]💡 HINTS:[/bold blue]")
            for hint in hints:
                if isinstance(hint, dict):
                    hint_class = hint.get("class", "unknown")
                    console.print(f"  • {hint_class}")
                    if hint_class == "dirac:Production":
                        # Use plugin system for display formatting
                        plugin_name = hint.get("input_dataset_plugin")
                        if plugin_name:
                            console.print(f"    [dim]Plugin:[/dim] {plugin_name}")
                            try:
                                from dirac_cwl.production import get_registry

                                plugin_cls = get_registry().get_plugin(plugin_name)
                                if plugin_cls:
                                    config = hint.get("input_dataset_config", {})
                                    plugin_instance = plugin_cls()
                                    for key, value in plugin_instance.format_hint_display(config):
                                        console.print(f"    [dim]{key}:[/dim] {value}")
                            except Exception:
                                pass  # Silently ignore plugin display errors
            console.print()

    except Exception as e:
        console.print(f"[yellow]⚠ Could not visualize workflow:[/yellow] {e}\n")


def check_and_generate_inputs(
    workflow_path: Path,
    inputs_path: Path | None,
    replica_map_path: Path | None,
    n_lfns: int | None = None,
    pick_smallest: bool = False,
    force: bool = False,
) -> tuple[Path | None, Path | None]:
    """Check if inputs and replica map need to be generated from dirac:Production hint.

    Uses the input dataset plugin system to generate inputs from experiment-specific
    data sources (e.g., LHCb Bookkeeping).

    Returns:
        Tuple of (inputs_path, replica_map_path) to use. If generation was not needed
        or failed, returns the original paths.
    """
    import yaml

    from dirac_cwl.production import ProductionHint, get_registry

    # Read the CWL file to check for Production hint
    try:
        with open(workflow_path, "r") as f:
            cwl_content = yaml.safe_load(f)
    except Exception as e:
        console.print(f"[yellow]⚠ Warning:[/yellow] Could not read workflow file: {e}")
        return inputs_path, replica_map_path

    # Check for dirac:Production hint with input_dataset_plugin
    hint = ProductionHint.from_cwl(cwl_content)

    if hint is None or hint.input_dataset_plugin is None:
        # No Production hint with plugin, nothing to auto-generate
        return inputs_path, replica_map_path

    # Determine default paths if not provided
    default_inputs = workflow_path.parent / f"{workflow_path.stem}-inputs.yml"
    default_replica_map = workflow_path.parent / f"{workflow_path.stem}-replica-map.json"

    actual_inputs = inputs_path or default_inputs
    actual_replica_map = replica_map_path or default_replica_map

    # Check if we need to generate
    inputs_exists = actual_inputs.exists()
    replica_map_exists = actual_replica_map.exists()

    # Determine if user wants to regenerate (specified --n-lfns or --pick-smallest-lfn)
    user_wants_regenerate = n_lfns is not None or pick_smallest

    # If inputs/replica_map were explicitly provided (not defaults), don't auto-generate
    if inputs_path is not None:
        if inputs_exists:
            console.print(f"[green]✓[/green] Using provided inputs file: {inputs_path}")
            return inputs_path, replica_map_path
        else:
            console.print(f"[red]✗ Error:[/red] Provided inputs file does not exist: {inputs_path}")
            raise typer.Exit(1)

    if replica_map_path is not None:
        if replica_map_exists:
            console.print(f"[green]✓[/green] Using provided replica map file: {replica_map_path}")
            return inputs_path, replica_map_path
        else:
            console.print(f"[red]✗ Error:[/red] Provided replica map file does not exist: {replica_map_path}")
            raise typer.Exit(1)

    # If both exist and user didn't request regeneration, use existing
    if inputs_exists and replica_map_exists and not user_wants_regenerate:
        console.print(
            f"[green]✓[/green] Using existing inputs ({actual_inputs}) and replica map ({actual_replica_map})"
        )
        return actual_inputs, actual_replica_map

    # If files exist but user wants to regenerate, prompt for confirmation
    if (inputs_exists or replica_map_exists) and user_wants_regenerate:
        if not force:
            console.print("\n[yellow]⚠ WARNING:[/yellow] The following files will be overwritten:")
            if inputs_exists:
                console.print(f"  - {actual_inputs}")
            if replica_map_exists:
                console.print(f"  - {actual_replica_map}")
            console.print()
            if not typer.confirm("Continue and overwrite?", default=False):
                console.print("[yellow]Aborted.[/yellow] Using existing files.")
                return (
                    actual_inputs if inputs_exists else None,
                    actual_replica_map if replica_map_exists else None,
                )
        else:
            console.print("[green]✓[/green] Force mode: Overwriting existing files")
            if inputs_exists:
                console.print(f"  - {actual_inputs}")
            if replica_map_exists:
                console.print(f"  - {actual_replica_map}")

    # Validate flags
    if pick_smallest and n_lfns is None:
        console.print("[red]✗ Error:[/red] --pick-smallest-lfn requires --n-lfns to be specified")
        raise typer.Exit(1)

    # Auto-generate using the plugin system
    console.print()
    console.print(
        Panel.fit(
            f"Auto-generating inputs using {hint.input_dataset_plugin}",
            border_style="cyan",
        )
    )
    console.print(f"Workflow: [cyan]{workflow_path}[/cyan]")
    console.print(f"Plugin: [cyan]{hint.input_dataset_plugin}[/cyan]")
    if n_lfns is not None:
        mode = "picking smallest" if pick_smallest else "sampling"
        console.print(f"Number of LFNs: [cyan]{n_lfns}[/cyan] ([dim]{mode} mode[/dim])")
    else:
        console.print("Number of LFNs: [cyan]ALL available files[/cyan]")
    console.print()

    try:
        # Get the plugin from registry and generate inputs
        registry = get_registry()
        plugin = registry.instantiate(hint)

        console.print(f"[green]✓[/green] Running {plugin.name()} plugin...")

        generated_inputs, generated_replica_map = plugin.generate_inputs(
            workflow_path=workflow_path,
            config=hint.input_dataset_config,
            output_dir=workflow_path.parent,
            n_lfns=n_lfns,
            pick_smallest=pick_smallest,
        )

        if generated_inputs and generated_replica_map:
            console.print("\n[green]✅ Successfully auto-generated inputs and replica map[/green]\n")
            return generated_inputs, generated_replica_map
        else:
            console.print("\n[yellow]⚠ Plugin returned no files[/yellow]\n")
            return inputs_path, replica_map_path

    except Exception as e:
        console.print(f"\n[red]❌ Failed to auto-generate inputs and replica map:[/red] {e}")
        console.print("\n[dim]Full traceback:[/dim]")
        console.print_exception()
        raise typer.Exit(1) from None


@app.command()
def main(
    ctx: typer.Context,
    workflow: Path = typer.Argument(..., help="Path to CWL workflow file", exists=True),
    inputs: Path | None = typer.Argument(None, help="Path to inputs YAML file (optional)"),
    outdir: Path = typer.Option(None, help="Output directory (default: current directory)"),
    tmpdir_prefix: Path = typer.Option(None, help="Temporary directory prefix"),
    leave_tmpdir: bool = typer.Option(False, help="Keep temporary directories"),
    replica_map: Path = typer.Option(None, help="Path to global replica map JSON file"),
    n_lfns: int = typer.Option(
        None,
        "--n-lfns",
        help="Number of LFNs to retrieve when auto-generating inputs (default: all available LFNs)",
    ),
    pick_smallest_lfn: bool = typer.Option(
        False,
        "--pick-smallest-lfn",
        help="Pick the smallest file(s) for faster testing (requires --n-lfns)",
    ),
    force_regenerate: bool = typer.Option(
        False,
        "--force-regenerate",
        help="Force regeneration of inputs/replica map without confirmation",
    ),
    print_workflow: bool = typer.Option(
        False, "--print-workflow", help="Print the workflow structure before execution"
    ),
    preserve_environment: list[str] = typer.Option(
        [],
        "--preserve-environment",
        help="Preserve specific environment variable when running CommandLineTools. May be provided multiple times.",
    ),
    preserve_entire_environment: bool = typer.Option(
        False,
        "--preserve-entire-environment",
        help="Preserve entire host environment when running CommandLineTools.",
    ),
    debug: bool = typer.Option(False, help="Enable debug logging"),
    verbose: bool = typer.Option(False, help="Enable verbose logging"),
    parallel: bool = typer.Option(False, help="Run jobs in parallel"),
    version: bool = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show version information",
    ),
):
    r"""Run CWL workflows with DIRAC executor.

    \b
    Workflow-specific parameters can be passed directly and will be forwarded to the workflow:
        dirac-cwl-run workflow.cwl --event-type 27165175 --run-number 12345

    \b
    Parameters recognized by dirac-cwl-run (like --outdir, --debug) must come before workflow parameters.
    If there's ambiguity, use -- to separate:
        dirac-cwl-run workflow.cwl --outdir myout -- --event-type 27165175
    """
    # Configure logging to use UTC
    configure_utc_logging()

    # Record start time
    start_time = datetime.now(timezone.utc)

    # Extract workflow parameters from context (passed after known options)
    workflow_params = ctx.args if ctx.args else []

    # Check and auto-generate inputs and catalog if needed
    actual_inputs, actual_replica_map = check_and_generate_inputs(
        workflow_path=workflow,
        inputs_path=inputs,
        replica_map_path=replica_map,
        n_lfns=n_lfns,
        pick_smallest=pick_smallest_lfn,
        force=force_regenerate,
    )

    # Build cwltool arguments
    cwltool_args = [
        "--outdir",
        str(outdir) if outdir else ".",
        "--disable-color",  # Disable ANSI color codes in logs
    ]

    if tmpdir_prefix:
        cwltool_args.extend(["--tmpdir-prefix", str(tmpdir_prefix)])

    if leave_tmpdir:
        cwltool_args.append("--leave-tmpdir")

    if debug:
        cwltool_args.append("--debug")
    elif verbose:
        cwltool_args.append("--verbose")

    if parallel:
        cwltool_args.append("--parallel")

    if preserve_entire_environment:
        cwltool_args.append("--preserve-entire-environment")
    else:
        for envvar in preserve_environment:
            cwltool_args.extend(["--preserve-environment", envvar])

    # Workflow printing - show our nice visualization
    if print_workflow:
        print_workflow_visualization(workflow)

    # Add workflow and inputs
    cwltool_args.append(str(workflow))
    if actual_inputs:
        cwltool_args.append(str(actual_inputs))

    # Add any extra workflow parameters passed by the user
    if workflow_params:
        cwltool_args.extend(workflow_params)

    try:
        # Import hook is installed by __init__.py before any cwltool import.
        from cwltool.context import LoadingContext
        from cwltool.main import main as cwltool_main

        from . import DiracExecutor
        from .tool import dirac_make_tool

        dirac_executor = DiracExecutor(global_map_path=actual_replica_map)

        # Display execution info
        console.print()
        console.print(
            Panel.fit(
                "[bold cyan]DIRAC CWL Workflow Executor[/bold cyan]",
                border_style="cyan",
            )
        )

        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("Key", style="bold")
        table.add_column("Value")

        table.add_row(
            "Start time (UTC):",
            f"[cyan]{start_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]",
        )
        table.add_row("CWL Workflow:", f"[cyan]{workflow.resolve()}[/cyan]")
        if actual_inputs:
            table.add_row("Input Parameter File:", f"[cyan]{actual_inputs.resolve()}[/cyan]")

        table.add_row("Current working directory:", f"[cyan]{Path.cwd()}[/cyan]")
        table.add_row(
            "Temporary dir prefix:",
            f"[cyan]{tmpdir_prefix if tmpdir_prefix else 'system default'}[/cyan]",
        )
        table.add_row(
            "Output directory:",
            f"[cyan]{Path(outdir).resolve() if outdir else '.'}[/cyan]",
        )

        console.print(table)
        console.print()
        console.print("[green]✓[/green] Using DIRAC executor with replica map management")

        if actual_replica_map:
            console.print(f"[green]✓[/green] Global replica map: [cyan]{actual_replica_map}[/cyan]")
        else:
            console.print("[yellow]⚠[/yellow] No replica map provided - will create empty replica map")

        # Show workflow parameters if provided
        if workflow_params:
            console.print(f"[green]✓[/green] Workflow parameters: [cyan]{' '.join(workflow_params)}[/cyan]")

        console.print()

        # Show execution start message
        console.print(
            Panel.fit(
                "[bold green]▶[/bold green] Starting workflow execution with cwltool...",
                border_style="green",
                padding=(0, 2),
            )
        )
        console.print()

        # Let cwltool manage its own logging (coloredlogs to stderr).
        # Only set up a handler for dirac-cwl-run so our executor messages
        # go to stdout without duplicating cwltool output.
        _dcr = logging.getLogger("dirac-cwl-run")
        _dcr.propagate = False
        _dcr_handler = logging.StreamHandler(sys.stdout)
        _dcr_handler.setFormatter(logging.Formatter("%(message)s"))
        _dcr.addHandler(_dcr_handler)
        _dcr.setLevel(logging.INFO)

        # Create LoadingContext with our custom tool factory so cwltool
        # uses DiracCommandLineTool (which supports custom path mappers)
        # instead of the default CommandLineTool.
        loading_context = LoadingContext()
        loading_context.construct_tool_object = dirac_make_tool

        exit_code = cwltool_main(
            argsl=cwltool_args,
            executor=dirac_executor,
            loadingContext=loading_context,
        )

        # Record end time and calculate duration
        end_time = datetime.now(timezone.utc)
        duration = end_time - start_time

        if exit_code == 0:
            console.print()
            console.print(
                Panel.fit(
                    "[bold green]✅ Workflow Execution Complete[/bold green]",
                    border_style="green",
                )
            )

            # Build results table
            results_table = Table(show_header=False, box=None, padding=(0, 2))
            results_table.add_column("Key", style="bold")
            results_table.add_column("Value")

            results_table.add_row("Status:", "[green]Success[/green]")
            results_table.add_row(
                "Start time (UTC):",
                f"[cyan]{start_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]",
            )
            results_table.add_row(
                "End time (UTC):",
                f"[cyan]{end_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]",
            )
            results_table.add_row("Duration:", f"[cyan]{str(duration).split('.')[0]}[/cyan]")
            results_table.add_row(
                "Output directory:",
                f"[cyan]{Path(outdir).resolve() if outdir else '.'}[/cyan]",
            )

            # Write final global replica map to output directory
            output_dir_path = Path(outdir).resolve() if outdir else Path.cwd()
            final_replica_map_path = output_dir_path / "replica_map.json"

            if dirac_executor.global_map:
                try:
                    final_replica_map_path.write_text(dirac_executor.global_map.model_dump_json(indent=2))
                    results_table.add_row("Final replica map:", f"[cyan]{final_replica_map_path}[/cyan]")
                    results_table.add_row(
                        "Replica map entries:",
                        f"[cyan]{len(dirac_executor.global_map.root)}[/cyan]",
                    )
                except Exception as e:
                    console.print(f"[yellow]⚠ Warning:[/yellow] Could not write final replica map: {e}")

            # Show original replica map if it was different
            if actual_replica_map and actual_replica_map.exists() and actual_replica_map != final_replica_map_path:
                results_table.add_row("Input replica map:", f"[dim][cyan]{actual_replica_map}[/cyan][/dim]")

            console.print(results_table)
            console.print()

        else:
            console.print()
            console.print(
                Panel.fit(
                    f"[bold red]❌ Workflow Execution Failed[/bold red]\n[dim]Exit code: {exit_code}[/dim]",
                    border_style="red",
                )
            )

            # Build failure table
            failure_table = Table(show_header=False, box=None, padding=(0, 2))
            failure_table.add_column("Key", style="bold")
            failure_table.add_column("Value")

            failure_table.add_row("Status:", "[red]Failed[/red]")
            failure_table.add_row(
                "Start time (UTC):",
                f"[cyan]{start_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]",
            )
            failure_table.add_row(
                "End time (UTC):",
                f"[cyan]{end_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]",
            )
            failure_table.add_row("Duration:", f"[cyan]{str(duration).split('.')[0]}[/cyan]")
            failure_table.add_row("Exit code:", f"[red]{exit_code}[/red]")

            console.print(failure_table)
            console.print()

        sys.exit(exit_code)

    except SystemExit:
        raise
    except Exception as e:
        console.print(f"\n[red]❌ Error executing workflow:[/red] {e}")
        console.print_exception()
        sys.exit(1)


def cli():
    """Entry point for the CLI when installed as a script."""
    app()


if __name__ == "__main__":
    cli()
