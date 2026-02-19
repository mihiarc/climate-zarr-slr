#!/usr/bin/env python
"""Monitor all queued GEE batch tasks and assemble CSVs as they complete.

Polls GEE task status, reads back completed assets, post-processes per
variable, merges, and writes one CSV per (model, scenario) combo.
Cleans up assets after read-back.

Usage:
    uv run python scripts/run_gee_batch_monitor.py
"""

import time
from collections import defaultdict
from pathlib import Path

import ee
import pandas as pd
from rich.console import Console
from rich.table import Table

from climate_zarr.gee.config import GEEConfig
from climate_zarr.gee.extract import (
    extract_to_dataframe,
    postprocess_variable_dataframe,
)
from climate_zarr.transform import merge_climate_dataframes

console = Console()

PROJECT_ID = "ee-chrismihiar"
ASSET_FOLDER = f"projects/{PROJECT_ID}/assets/climate_zarr_tmp"
OUTPUT_DIR = Path("climate_outputs/gee")
POLL_INTERVAL = 30
VARIABLES = ("pr", "tas", "tasmax", "tasmin")


def get_batch_tasks() -> list[dict]:
    """Get all batch tasks from GEE that belong to our pipeline."""
    all_tasks = ee.data.getTaskList()
    batch_tasks = []
    for task in all_tasks:
        desc = task.get("description", "")
        parts = desc.split("_")
        if len(parts) >= 4:
            task["_var"] = parts[0]
            task["_year"] = int(parts[1])
            task["_model"] = parts[2]
            task["_scenario"] = "_".join(parts[3:])
            batch_tasks.append(task)
    return batch_tasks


def print_status(batch_tasks: list[dict]) -> dict:
    """Print a status table and return counts by (model, scenario)."""
    groups = defaultdict(lambda: {"COMPLETED": 0, "RUNNING": 0, "READY": 0, "FAILED": 0, "total": 0})
    for task in batch_tasks:
        key = (task["_model"], task["_scenario"])
        state = task.get("state", "UNKNOWN")
        groups[key][state] = groups[key].get(state, 0) + 1
        groups[key]["total"] += 1

    table = Table(title="GEE Batch Export Status")
    table.add_column("Model / Scenario", style="bold")
    table.add_column("Total", justify="right")
    table.add_column("Done", justify="right", style="green")
    table.add_column("Running", justify="right", style="cyan")
    table.add_column("Queued", justify="right")
    table.add_column("Failed", justify="right", style="red")

    grand_total = 0
    grand_done = 0
    for key in sorted(groups):
        group = groups[key]
        grand_total += group["total"]
        grand_done += group["COMPLETED"]
        table.add_row(
            f"{key[0]} / {key[1]}",
            str(group["total"]),
            str(group["COMPLETED"]),
            str(group["RUNNING"]),
            str(group["READY"]),
            str(group["FAILED"]),
        )

    table.add_section()
    table.add_row("TOTAL", str(grand_total), str(grand_done), "", "", "")
    console.print(table)
    return groups


def read_and_assemble(model: str, scenario: str, batch_tasks: list[dict]) -> None:
    """Read back all completed assets for a model/scenario and write CSV."""
    relevant_tasks = [
        t for t in batch_tasks
        if t["_model"] == model and t["_scenario"] == scenario and t.get("state") == "COMPLETED"
    ]

    # Group by variable
    by_variable = defaultdict(list)
    for task in relevant_tasks:
        by_variable[task["_var"]].append(task)

    per_variable_dataframes = {}
    for variable_name in VARIABLES:
        variable_tasks = by_variable.get(variable_name, [])
        if not variable_tasks:
            console.print(f"  [yellow]{variable_name}: no completed tasks[/yellow]")
            continue

        console.print(f"  [cyan]Reading {variable_name}: {len(variable_tasks)} assets...[/cyan]")
        raw_frames = []
        for task in variable_tasks:
            asset_id = f"{ASSET_FOLDER}/{task['description']}"
            try:
                fc = ee.FeatureCollection(asset_id)
                raw_df = extract_to_dataframe(fc)
                if not raw_df.empty:
                    raw_frames.append(raw_df)
            except Exception as error:
                console.print(f"    [red]Failed to read {asset_id}: {error}[/red]")

        if not raw_frames:
            continue

        combined_raw = pd.concat(raw_frames, ignore_index=True)
        processed_df = postprocess_variable_dataframe(combined_raw, variable_name)
        per_variable_dataframes[variable_name] = processed_df
        console.print(
            f"  [green]{variable_name}: {len(processed_df)} rows "
            f"({processed_df['county_id'].nunique()} counties, "
            f"{processed_df['year'].nunique()} years)[/green]"
        )

    if not per_variable_dataframes:
        console.print(f"  [red]No data for {model}/{scenario}[/red]")
        return

    merged_df = merge_climate_dataframes(per_variable_dataframes)
    output_path = OUTPUT_DIR / f"conus_{scenario}_{model}_climate_stats.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(output_path, index=False)
    console.print(
        f"  [bold green]Saved: {output_path} ({len(merged_df)} rows)[/bold green]"
    )

    # Cleanup assets
    console.print(f"  [dim]Cleaning up {len(relevant_tasks)} assets...[/dim]")
    cleaned = 0
    for task in relevant_tasks:
        asset_id = f"{ASSET_FOLDER}/{task['description']}"
        try:
            ee.data.deleteAsset(asset_id)
            cleaned += 1
        except Exception:
            pass
    console.print(f"  [dim]Cleaned {cleaned}/{len(relevant_tasks)} assets[/dim]")


def main():
    ee.Initialize(project=PROJECT_ID)
    console.print("[bold]GEE Batch Monitor[/bold]")
    console.print(f"Polling every {POLL_INTERVAL}s. Press Ctrl+C to stop.\n")

    assembled = set()  # (model, scenario) combos already written to CSV

    try:
        while True:
            batch_tasks = get_batch_tasks()
            groups = print_status(batch_tasks)

            # Check for newly completed groups
            for (model, scenario), counts in groups.items():
                key = (model, scenario)
                if key in assembled:
                    continue

                if counts["COMPLETED"] == counts["total"] and counts["total"] > 0:
                    console.print(
                        f"\n[bold green]{model} / {scenario}: "
                        f"all {counts['total']} tasks complete![/bold green]"
                    )
                    read_and_assemble(model, scenario, batch_tasks)
                    assembled.add(key)

            # Check if everything is done
            total_tasks = sum(g["total"] for g in groups.values())
            total_done = sum(g["COMPLETED"] for g in groups.values())
            total_failed = sum(g["FAILED"] for g in groups.values())
            total_terminal = total_done + total_failed

            if total_terminal == total_tasks:
                console.print("\n[bold green]All tasks finished![/bold green]")
                # Assemble any remaining groups with partial results
                for (model, scenario), counts in groups.items():
                    if (model, scenario) not in assembled and counts["COMPLETED"] > 0:
                        console.print(
                            f"\n[yellow]{model}/{scenario}: assembling partial "
                            f"results ({counts['COMPLETED']}/{counts['total']})[/yellow]"
                        )
                        read_and_assemble(model, scenario, batch_tasks)
                        assembled.add((model, scenario))
                break

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped monitoring. Tasks continue running on GEE.[/yellow]")
        console.print("Re-run this script to resume monitoring.")


if __name__ == "__main__":
    main()
