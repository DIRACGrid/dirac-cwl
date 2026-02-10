#!/usr/bin/env python3
"""Pi estimation gathering module using Monte Carlo results."""

import math
from typing import List

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def process(files: List[str] = typer.Argument(..., help="Paths to the input files")):
    """Process the input points and estimate the value of Pi using the Monte Carlo method."""
    inside_circle = 0
    total_points = 0

    # Read points from file and check if they fall within the unit circle
    with open(files[0], "r") as f:
        for line in f:
            x, y = map(float, line.split())
            if math.sqrt(x**2 + y**2) <= 1:
                inside_circle += 1
            total_points += 1

    # Estimate Pi
    pi_estimate = 4 * (inside_circle / total_points)

    # Write the result to a file
    output_name = "result_final.sim"
    with open(output_name, "w") as f:
        f.write(f"Approximation of Pi: {pi_estimate}\n")

    console.print(f"Pi approximation: [bold yellow]{pi_estimate}[/bold yellow]")
    return output_name


if __name__ == "__main__":
    app()
