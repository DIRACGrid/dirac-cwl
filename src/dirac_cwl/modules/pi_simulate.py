#!/usr/bin/env python3
"""Monte Carlo simulation module for Pi estimation."""

import random

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def simulate(num_points: int = typer.Argument(..., help="Number of random points to generate")):
    """Simulate random points inside a square (Monte Carlo method)."""
    points = []

    for _ in range(num_points):
        x, y = random.uniform(-1, 1), random.uniform(-1, 1)
        points.append((x, y))

    # Save points to file
    output_path = "result.sim"
    with open(output_path, "w") as f:
        for point in points:
            f.write(f"{point[0]} {point[1]}\n")

    console.print(f"Generated [bold green]{num_points}[/bold green] random points.")
    return output_path


if __name__ == "__main__":
    app()
