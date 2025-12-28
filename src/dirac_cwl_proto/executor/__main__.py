"""CLI tool for running CWL workflows with DIRAC executor.

This command-line tool runs CWL workflows using the DiracExecutor, which handles
replica catalog management for input and output files.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

# Create Typer app with context settings to allow extra arguments
app = typer.Typer(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)


# Configure logging to use UTC
def configure_utc_logging():
    """Configure logging to use UTC timestamps."""
    logging.Formatter.converter = lambda *args: datetime.now(timezone.utc).timetuple()


def version_callback(value: bool):
    """Callback for --version flag."""
    if value:
        console.print("[cyan]DIRAC CWL Executor[/cyan]")
        console.print("Version: [green]0.1.0[/green]")
        console.print("Built with cwltool and DIRAC integration")
        raise typer.Exit()


def print_workflow_visualization(workflow_path: Path):
    """Print a nice visualization of the workflow structure with graph representation."""
    import yaml

    try:
        with open(workflow_path, "r") as f:
            cwl = yaml.safe_load(f)

        console.print()
        console.print(Panel.fit(
            f"[bold cyan]Workflow Visualization[/bold cyan]\n[dim]{workflow_path.name}[/dim]",
            border_style="cyan"
        ))

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
                    is_last = (i == len(step_list) - 1)

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
                                source = in_source if isinstance(in_source, str) else in_source.get("source", "?") if isinstance(in_source, dict) else "?"
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
                    if hint_class == "dirac:inputDataset":
                        event_type = hint.get("event_type", "?")
                        config_name = hint.get("conditions_dict", {}).get("configName", "?")
                        console.print(f"    [dim]EventType:[/dim] {event_type}")
                        console.print(f"    [dim]Config:[/dim] {config_name}")
            console.print()

    except Exception as e:
        console.print(f"[yellow]⚠ Could not visualize workflow:[/yellow] {e}\n")


def check_and_generate_inputs(
    workflow_path: Path,
    inputs_path: Path | None,
    catalog_path: Path | None,
    n_lfns: int | None = None,
    pick_smallest: bool = False,
    force: bool = False,
) -> tuple[Path | None, Path | None]:
    """Check if inputs and catalog need to be generated from inputDataset hint.

    Returns:
        Tuple of (inputs_path, catalog_path) to use. If generation was not needed
        or failed, returns the original paths.
    """
    import yaml

    # Read the CWL file to check for inputDataset hint
    try:
        with open(workflow_path, "r") as f:
            cwl_content = yaml.safe_load(f)
    except Exception as e:
        console.print(f"[yellow]⚠ Warning:[/yellow] Could not read workflow file: {e}")
        return inputs_path, catalog_path

    # Check for inputDataset hint
    hints = cwl_content.get("hints", [])
    has_input_dataset = any(hint.get("class") == "dirac:inputDataset" for hint in hints)

    if not has_input_dataset:
        # No inputDataset hint, nothing to auto-generate
        return inputs_path, catalog_path

    # Determine default paths if not provided
    default_inputs = workflow_path.parent / f"{workflow_path.stem}-inputs.yml"
    default_catalog = workflow_path.parent / f"{workflow_path.stem}-catalog.json"

    actual_inputs = inputs_path or default_inputs
    actual_catalog = catalog_path or default_catalog

    # Check if we need to generate
    inputs_exists = actual_inputs.exists()
    catalog_exists = actual_catalog.exists()

    # Determine if user wants to regenerate (specified --n-lfns or --pick-smallest-lfn)
    user_wants_regenerate = n_lfns is not None or pick_smallest

    # If inputs/catalog were explicitly provided (not defaults), don't auto-generate
    if inputs_path is not None:
        if inputs_exists:
            console.print(f"[green]✓[/green] Using provided inputs file: {inputs_path}")
            return inputs_path, catalog_path
        else:
            console.print(f"[yellow]⚠ Warning:[/yellow] Provided inputs file does not exist: {inputs_path}")
            return inputs_path, catalog_path

    if catalog_path is not None:
        if catalog_exists:
            console.print(f"[green]✓[/green] Using provided catalog file: {catalog_path}")
            return inputs_path, catalog_path
        else:
            console.print(f"[yellow]⚠ Warning:[/yellow] Provided catalog file does not exist: {catalog_path}")
            return inputs_path, catalog_path

    # If both exist and user didn't request regeneration, use existing
    if inputs_exists and catalog_exists and not user_wants_regenerate:
        console.print(
            f"[green]✓[/green] Using existing inputs ({actual_inputs}) and catalog ({actual_catalog})"
        )
        return actual_inputs, actual_catalog

    # If files exist but user wants to regenerate, prompt for confirmation
    if (inputs_exists or catalog_exists) and user_wants_regenerate:
        if not force:
            console.print("\n[yellow]⚠ WARNING:[/yellow] The following files will be overwritten:")
            if inputs_exists:
                console.print(f"  - {actual_inputs}")
            if catalog_exists:
                console.print(f"  - {actual_catalog}")
            console.print()

            if not typer.confirm("Continue and overwrite?", default=False):
                console.print("[yellow]Aborted.[/yellow] Using existing files.")
                return (
                    actual_inputs if inputs_exists else None,
                    actual_catalog if catalog_exists else None,
                )
        else:
            console.print(f"[green]✓[/green] Force mode: Overwriting existing files")
            if inputs_exists:
                console.print(f"  - {actual_inputs}")
            if catalog_exists:
                console.print(f"  - {actual_catalog}")

    # Validate flags
    if pick_smallest and n_lfns is None:
        console.print(
            "[yellow]⚠ Warning:[/yellow] --pick-smallest-lfn requires --n-lfns to be specified, ignoring flag"
        )
        pick_smallest = False

    # Auto-generate if we have inputDataset hint and files don't exist
    console.print()
    console.print(Panel.fit(
        "Auto-generating inputs and catalog from inputDataset hint",
        border_style="cyan"
    ))
    console.print(f"Workflow: [cyan]{workflow_path}[/cyan]")
    if n_lfns is not None:
        mode = "picking smallest" if pick_smallest else "sampling"
        console.print(f"Number of LFNs: [cyan]{n_lfns}[/cyan] ([dim]{mode} mode[/dim])")
    else:
        console.print("Number of LFNs: [cyan]ALL available files[/cyan]")
    console.print()

    try:
        # Import here to avoid initialization issues
        # Initialize DIRAC
        import DIRAC

        from dirac_cwl_proto.job.replica_catalog import dirac_make_replica_catalog
        from dirac_cwl_proto.lhcb.generate_replica_catalog import (
            InputDataset,
            do_bkquery,
        )

        DIRAC.initialize()

        # Find the inputDataset hint
        input_dataset_dict = None
        for hint in hints:
            if hint.get("class") == "dirac:inputDataset":
                input_dataset_dict = hint
                break

        # Parse into InputDataset model
        input_dataset = InputDataset.model_validate(input_dataset_dict)

        console.print("[green]✓[/green] Found inputDataset hint:")
        console.print(f"  Event Type: [cyan]{input_dataset.event_type}[/cyan]")
        console.print(
            f"  Config: [cyan]{input_dataset.conditions_dict.configName}/{input_dataset.conditions_dict.configVersion}[/cyan]"
        )

        # Query Bookkeeping for LFNs
        if n_lfns is not None:
            if pick_smallest:
                console.print(f"\n[green]✓[/green] Querying Bookkeeping for {n_lfns} smallest LFN(s)...")
            else:
                console.print(f"\n[green]✓[/green] Querying Bookkeeping for {n_lfns} LFN(s)...")
        else:
            console.print("\n[green]✓[/green] Querying Bookkeeping for all available LFNs...")

        with console.status("[cyan]Querying Bookkeeping...[/cyan]"):
            lfns_list = do_bkquery(
                input_dataset, numTestLFNs=n_lfns, pick_smallest=pick_smallest
            )

        # Generate replica catalog
        console.print("[green]✓[/green] Generating replica catalog...")
        replica_catalog = dirac_make_replica_catalog(lfns_list)

        # Generate CWL input YAML file
        cwl_inputs = {
            "input-data": [
                {
                    "class": "File",
                    "location": f"LFN:{lfn}",
                }
                for lfn in lfns_list
            ]
        }

        # Write outputs
        console.print(f"[green]✓[/green] Writing inputs to: [cyan]{default_inputs}[/cyan]")
        with open(default_inputs, "w") as f:
            yaml.dump(cwl_inputs, f, default_flow_style=False, sort_keys=False)

        console.print(f"[green]✓[/green] Writing catalog to: [cyan]{default_catalog}[/cyan]")
        with open(default_catalog, "w") as f:
            f.write(replica_catalog.model_dump_json(indent=2))

        console.print(f"\n[green]✅ Successfully auto-generated inputs and catalog[/green]")
        console.print(f"   Retrieved [cyan]{len(lfns_list)}[/cyan] LFN(s)\n")

        return default_inputs, default_catalog

    except Exception as e:
        import traceback

        console.print(f"\n[red]❌ Failed to auto-generate inputs and catalog:[/red] {e}")
        console.print("\n[dim]Full traceback:[/dim]")
        console.print_exception()
        console.print("\n[yellow]Falling back to manual mode - please provide inputs and catalog[/yellow]\n")
        return inputs_path, catalog_path


@app.command()
def main(
    ctx: typer.Context,
    workflow: Path = typer.Argument(..., help="Path to CWL workflow file", exists=True),
    inputs: Path | None = typer.Argument(None, help="Path to inputs YAML file (optional)"),
    outdir: Path = typer.Option(None, help="Output directory (default: current directory)"),
    tmpdir_prefix: Path = typer.Option(None, help="Temporary directory prefix"),
    leave_tmpdir: bool = typer.Option(False, help="Keep temporary directories"),
    replica_catalog: Path = typer.Option(None, help="Path to master replica catalog JSON file"),
    n_lfns: int = typer.Option(
        None,
        "--n-lfns",
        help="Number of LFNs to retrieve when auto-generating inputs (default: all available LFNs)",
    ),
    pick_smallest_lfn: bool = typer.Option(
        False, "--pick-smallest-lfn", help="Pick the smallest file(s) for faster testing (requires --n-lfns)"
    ),
    force_regenerate: bool = typer.Option(
        False, "--force-regenerate", help="Force regeneration of inputs/catalog without confirmation"
    ),
    provenance: Path = typer.Option(
        None, help="Save provenance to specified folder as a Research Object (opt-in)"
    ),
    enable_user_provenance: bool = typer.Option(
        True, "--enable-user-provenance/--disable-user-provenance",
        help="Record user account info as part of provenance"
    ),
    enable_host_provenance: bool = typer.Option(
        True, "--enable-host-provenance/--disable-host-provenance",
        help="Record host info as part of provenance"
    ),
    orcid: str = typer.Option(None, help="Record user ORCID identifier as part of provenance"),
    full_name: str = typer.Option(
        None, help="Record full name of user/system as part of provenance (e.g., 'LHCb Production System')"
    ),
    print_workflow: bool = typer.Option(False, "--print-workflow", help="Print the workflow structure before execution"),
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
    """Run CWL workflows with DIRAC executor and replica catalog management.

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
    actual_inputs, actual_catalog = check_and_generate_inputs(
        workflow_path=workflow,
        inputs_path=inputs,
        catalog_path=replica_catalog,
        n_lfns=n_lfns,
        pick_smallest=pick_smallest_lfn,
        force=force_regenerate,
    )

    # Setup provenance only if explicitly requested
    actual_provenance = provenance
    # Don't auto-enable provenance - only use if explicitly set via --provenance flag
    # (auto-provenance interferes with custom FsAccess)

    # Build cwltool arguments
    cwltool_args = [
        "--outdir",
        str(outdir) if outdir else ".",
        "--disable-color",  # Disable ANSI color codes in logs
    ]

    # Preserve environment variables needed for local AnalysisProductions testing
    import os
    if "CMAKE_PREFIX_PATH" in os.environ:
        cwltool_args.extend(["--preserve-environment", "CMAKE_PREFIX_PATH"])
    if "ANALYSIS_PRODUCTIONS_DYNAMIC" in os.environ:
        cwltool_args.extend(["--preserve-environment", "ANALYSIS_PRODUCTIONS_DYNAMIC"])

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

    # Workflow printing - show our nice visualization
    if print_workflow:
        print_workflow_visualization(workflow)

    # Provenance options
    if actual_provenance:
        cwltool_args.extend(["--provenance", str(actual_provenance)])

        # User provenance (enabled by default)
        if enable_user_provenance:
            cwltool_args.append("--enable-user-provenance")
        else:
            cwltool_args.append("--disable-user-provenance")

        # Host provenance (enabled by default)
        if enable_host_provenance:
            cwltool_args.append("--enable-host-provenance")
        else:
            cwltool_args.append("--disable-host-provenance")

        # Optional ORCID
        if orcid:
            cwltool_args.extend(["--orcid", orcid])

        # Optional full name
        if full_name:
            cwltool_args.extend(["--full-name", full_name])

    # Add workflow and inputs
    cwltool_args.append(str(workflow))
    if actual_inputs:
        cwltool_args.append(str(actual_inputs))

    # Add any extra workflow parameters passed by the user
    if workflow_params:
        cwltool_args.extend(workflow_params)

    try:
        # Create our custom DIRAC executor with replica catalog support
        from cwltool.main import main as cwltool_main

        from dirac_cwl_proto.executor import DiracExecutor

        dirac_executor = DiracExecutor(master_catalog_path=actual_catalog)

        # Display execution info
        console.print()
        console.print(Panel.fit(
            "[bold cyan]DIRAC CWL Workflow Executor[/bold cyan]",
            border_style="cyan"
        ))

        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("Key", style="bold")
        table.add_column("Value")

        table.add_row("Start time (UTC):", f"[cyan]{start_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]")
        table.add_row("CWL Workflow:", f"[cyan]{workflow.resolve()}[/cyan]")
        if actual_inputs:
            table.add_row("Input Parameter File:", f"[cyan]{actual_inputs.resolve()}[/cyan]")

        table.add_row("Current working directory:", f"[cyan]{Path.cwd()}[/cyan]")
        table.add_row("Temporary dir prefix:", f"[cyan]{tmpdir_prefix if tmpdir_prefix else 'system default'}[/cyan]")
        table.add_row("Output directory:", f"[cyan]{Path(outdir).resolve() if outdir else '.'}[/cyan]")

        console.print(table)
        console.print()
        console.print("[green]✓[/green] Using DIRAC executor with replica catalog management")

        if actual_catalog:
            console.print(f"[green]✓[/green] Master catalog: [cyan]{actual_catalog}[/cyan]")
        else:
            console.print("[yellow]⚠[/yellow] No replica catalog provided - will create empty catalog")

        # Show workflow parameters if provided
        if workflow_params:
            console.print(f"[green]✓[/green] Workflow parameters: [cyan]{' '.join(workflow_params)}[/cyan]")

        # Show provenance info
        if actual_provenance:
            console.print(f"[green]✓[/green] Recording provenance to: [cyan]{actual_provenance}[/cyan]")
            prov_details = []
            if enable_user_provenance:
                prov_details.append("user info")
            if enable_host_provenance:
                prov_details.append("host info")
            if full_name:
                prov_details.append(f"full name: {full_name}")
            if orcid:
                prov_details.append(f"ORCID: {orcid}")
            if prov_details:
                console.print(f"  Recording: [dim]{', '.join(prov_details)}[/dim]")

        console.print()

        # Show execution start message
        console.print(Panel.fit(
            "[bold green]▶[/bold green] Starting workflow execution with cwltool...",
            border_style="green",
            padding=(0, 2)
        ))
        console.print()

        # Run cwltool with our custom executor
        null_handler = logging.NullHandler()

        exit_code = cwltool_main(
            argsl=cwltool_args,
            executor=dirac_executor,
            logger_handler=null_handler,
        )

        # Record end time and calculate duration
        end_time = datetime.now(timezone.utc)
        duration = end_time - start_time

        if exit_code == 0:
            console.print()
            console.print(Panel.fit(
                "[bold green]✅ Workflow Execution Complete[/bold green]",
                border_style="green"
            ))

            # Build results table
            results_table = Table(show_header=False, box=None, padding=(0, 2))
            results_table.add_column("Key", style="bold")
            results_table.add_column("Value")

            results_table.add_row("Status:", "[green]Success[/green]")
            results_table.add_row("Start time (UTC):", f"[cyan]{start_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]")
            results_table.add_row("End time (UTC):", f"[cyan]{end_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]")
            results_table.add_row("Duration:", f"[cyan]{str(duration).split('.')[0]}[/cyan]")
            results_table.add_row("Output directory:", f"[cyan]{Path(outdir).resolve() if outdir else '.'}[/cyan]")

            # Write final master catalog to output directory
            output_dir_path = Path(outdir).resolve() if outdir else Path.cwd()
            final_catalog_path = output_dir_path / "replica_catalog.json"

            if dirac_executor.master_catalog:
                try:
                    final_catalog_path.write_text(
                        dirac_executor.master_catalog.model_dump_json(indent=2)
                    )
                    results_table.add_row("Final replica catalog:", f"[cyan]{final_catalog_path}[/cyan]")
                    results_table.add_row("Catalog entries:", f"[cyan]{len(dirac_executor.master_catalog.root)}[/cyan]")
                except Exception as e:
                    console.print(f"[yellow]⚠ Warning:[/yellow] Could not write final catalog: {e}")

            # Show original catalog if it was different
            if actual_catalog and actual_catalog.exists() and actual_catalog != final_catalog_path:
                results_table.add_row("Input catalog:", f"[dim][cyan]{actual_catalog}[/cyan][/dim]")

            # Show provenance info if enabled
            if actual_provenance and actual_provenance.exists():
                results_table.add_row("Provenance:", f"[cyan]{actual_provenance}[/cyan]")

            console.print(results_table)
            console.print()

        else:
            console.print()
            console.print(Panel.fit(
                f"[bold red]❌ Workflow Execution Failed[/bold red]\n[dim]Exit code: {exit_code}[/dim]",
                border_style="red"
            ))

            # Build failure table
            failure_table = Table(show_header=False, box=None, padding=(0, 2))
            failure_table.add_column("Key", style="bold")
            failure_table.add_column("Value")

            failure_table.add_row("Status:", "[red]Failed[/red]")
            failure_table.add_row("Start time (UTC):", f"[cyan]{start_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]")
            failure_table.add_row("End time (UTC):", f"[cyan]{end_time.strftime('%Y-%m-%d %H:%M:%S')}[/cyan]")
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
