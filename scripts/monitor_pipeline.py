#!/usr/bin/env python
"""
Real-time pipeline monitoring dashboard for climate data processing.
Shows live status of Zarr conversions and statistics processing.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import time

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.text import Text

console = Console()

# Configuration
VARIABLES = ["pr", "tas", "tasmax", "tasmin"]
REGIONS = ["conus", "alaska", "hawaii", "puerto_rico", "guam"]
SCENARIOS = ["historical", "ssp126", "ssp245", "ssp370", "ssp585"]

BASE_ZARR_PATH = Path("/Volumes/SSD1TB/climate_outputs/zarr")
BASE_STATS_PATH = Path("/Volumes/SSD1TB/climate_outputs/stats")


def check_zarr_exists(variable: str, region: str, scenario: str) -> bool:
    """Check if a Zarr store exists and is valid."""
    zarr_path = (
        BASE_ZARR_PATH
        / variable
        / region
        / scenario
        / f"{region}_{scenario}_{variable}_daily.zarr"
    )
    # Check for .zarray file which indicates a valid Zarr store
    return (zarr_path / ".zarray").exists()


def check_stats_exists(variable: str, region: str, scenario: str) -> bool:
    """Check if statistics CSV exists."""
    stats_path = (
        BASE_STATS_PATH
        / variable
        / region
        / scenario
        / f"{region}_{scenario}_{variable}_stats_threshold*.csv"
    )
    # Use glob to find any matching file
    return len(list(stats_path.parent.glob(stats_path.name))) > 0 if stats_path.parent.exists() else False


def get_zarr_size(variable: str, region: str, scenario: str) -> str:
    """Get the size of a Zarr store."""
    zarr_path = (
        BASE_ZARR_PATH
        / variable
        / region
        / scenario
        / f"{region}_{scenario}_{variable}_daily.zarr"
    )
    if not zarr_path.exists():
        return "N/A"

    # Get directory size
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(zarr_path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if os.path.exists(filepath):
                total_size += os.path.getsize(filepath)

    # Format size
    if total_size < 1024:
        return f"{total_size}B"
    elif total_size < 1024 * 1024:
        return f"{total_size / 1024:.1f}KB"
    elif total_size < 1024 * 1024 * 1024:
        return f"{total_size / (1024 * 1024):.1f}MB"
    else:
        return f"{total_size / (1024 * 1024 * 1024):.2f}GB"


def get_completion_stats() -> Dict[str, Dict[str, int]]:
    """Get completion statistics for all scenarios."""
    stats = {}

    for scenario in SCENARIOS:
        total = len(VARIABLES) * len(REGIONS)
        zarr_complete = 0
        stats_complete = 0

        for variable in VARIABLES:
            for region in REGIONS:
                if check_zarr_exists(variable, region, scenario):
                    zarr_complete += 1
                if check_stats_exists(variable, region, scenario):
                    stats_complete += 1

        stats[scenario] = {
            "total": total,
            "zarr_complete": zarr_complete,
            "stats_complete": stats_complete,
            "zarr_percent": (zarr_complete / total) * 100,
            "stats_percent": (stats_complete / total) * 100,
        }

    return stats


def create_overview_table() -> Table:
    """Create overview table of all scenarios."""
    stats = get_completion_stats()

    table = Table(title="📊 Pipeline Overview", show_header=True, header_style="bold cyan")
    table.add_column("Scenario", style="cyan", width=12)
    table.add_column("Zarr Stores", justify="right", width=15)
    table.add_column("Progress", width=20)
    table.add_column("Statistics", justify="right", width=15)
    table.add_column("Progress", width=20)

    for scenario in SCENARIOS:
        data = stats[scenario]

        # Zarr progress bar
        zarr_complete = data["zarr_complete"]
        zarr_total = data["total"]
        zarr_pct = data["zarr_percent"]

        zarr_status = f"{zarr_complete}/{zarr_total}"
        zarr_bar = "█" * int(zarr_pct / 5) + "░" * (20 - int(zarr_pct / 5))

        if zarr_pct == 100:
            zarr_color = "green"
        elif zarr_pct > 0:
            zarr_color = "yellow"
        else:
            zarr_color = "red"

        # Stats progress bar
        stats_complete = data["stats_complete"]
        stats_pct = data["stats_percent"]

        stats_status = f"{stats_complete}/{zarr_total}"
        stats_bar = "█" * int(stats_pct / 5) + "░" * (20 - int(stats_pct / 5))

        if stats_pct == 100:
            stats_color = "green"
        elif stats_pct > 0:
            stats_color = "yellow"
        else:
            stats_color = "red"

        table.add_row(
            scenario.upper(),
            f"[{zarr_color}]{zarr_status}[/{zarr_color}]",
            f"[{zarr_color}]{zarr_bar}[/{zarr_color}]",
            f"[{stats_color}]{stats_status}[/{stats_color}]",
            f"[{stats_color}]{stats_bar}[/{stats_color}]",
        )

    return table


def create_detailed_table(scenario: str) -> Table:
    """Create detailed table for a specific scenario."""
    table = Table(
        title=f"📋 Detailed Status: {scenario.upper()}",
        show_header=True,
        header_style="bold yellow",
    )
    table.add_column("Variable", style="cyan", width=10)
    table.add_column("Region", style="magenta", width=12)
    table.add_column("Zarr", justify="center", width=8)
    table.add_column("Size", justify="right", width=10)
    table.add_column("Stats", justify="center", width=8)

    for variable in VARIABLES:
        for region in REGIONS:
            zarr_exists = check_zarr_exists(variable, region, scenario)
            stats_exists = check_stats_exists(variable, region, scenario)

            zarr_status = "✅" if zarr_exists else "❌"
            stats_status = "✅" if stats_exists else "❌"

            size = get_zarr_size(variable, region, scenario) if zarr_exists else "N/A"

            # Color based on status
            if zarr_exists and stats_exists:
                row_style = "green"
            elif zarr_exists:
                row_style = "yellow"
            else:
                row_style = "red"

            table.add_row(
                variable.upper(),
                region.replace("_", " ").title(),
                zarr_status,
                size,
                stats_status,
                style=row_style,
            )

    return table


def create_storage_panel() -> Panel:
    """Create storage usage panel."""
    if BASE_ZARR_PATH.exists():
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(BASE_ZARR_PATH):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total_size += os.path.getsize(filepath)

        size_gb = total_size / (1024 * 1024 * 1024)
        content = f"[bold cyan]Total Zarr Storage:[/bold cyan] {size_gb:.2f} GB"
    else:
        content = "[yellow]Storage path not found[/yellow]"

    return Panel(content, title="💾 Storage", border_style="cyan")


def generate_dashboard() -> Layout:
    """Generate the complete dashboard layout."""
    layout = Layout()

    # Split into header, body, footer
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=5),
    )

    # Header
    header_text = Text("🌡️ Climate Data Pipeline Monitor", style="bold blue", justify="center")
    timestamp = Text(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", style="dim", justify="center")
    layout["header"].update(Panel(header_text + "\n" + timestamp, border_style="blue"))

    # Body - split into overview and detailed
    layout["body"].split_column(
        Layout(name="overview", size=12),
        Layout(name="detailed"),
    )

    layout["body"]["overview"].update(create_overview_table())

    # Show detailed view for currently processing scenario
    stats = get_completion_stats()
    active_scenario = None

    # Find scenario with incomplete zarr stores
    for scenario in SCENARIOS:
        if 0 < stats[scenario]["zarr_complete"] < stats[scenario]["total"]:
            active_scenario = scenario
            break

    if active_scenario:
        layout["body"]["detailed"].update(create_detailed_table(active_scenario))
    else:
        # Show first incomplete scenario or historical
        for scenario in SCENARIOS:
            if stats[scenario]["zarr_complete"] < stats[scenario]["total"]:
                active_scenario = scenario
                break

        if active_scenario:
            layout["body"]["detailed"].update(create_detailed_table(active_scenario))
        else:
            layout["body"]["detailed"].update(
                Panel("[green]All scenarios complete![/green]", border_style="green")
            )

    # Footer
    layout["footer"].update(create_storage_panel())

    return layout


def main():
    """Main monitoring loop."""
    console.clear()

    try:
        with Live(generate_dashboard(), refresh_per_second=0.5, screen=True) as live:
            while True:
                time.sleep(5)  # Update every 5 seconds
                live.update(generate_dashboard())
    except KeyboardInterrupt:
        console.print("\n[yellow]Monitoring stopped by user[/yellow]")
        sys.exit(0)


if __name__ == "__main__":
    main()
