#!/usr/bin/env python
"""
Monitoring script to track progress of the batch county statistics processing.

This script monitors:
- Number of output files generated
- Total processing progress
- System resource usage
- Processing speed estimates
"""

import os
import time
import psutil
from pathlib import Path
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()


def get_processing_stats(stats_dir: Path):
    """Get current processing statistics."""
    if not stats_dir.exists():
        return {}
    
    stats = {}
    total_files = 0
    total_size = 0
    
    for variable_dir in stats_dir.iterdir():
        if not variable_dir.is_dir():
            continue
            
        variable = variable_dir.name
        stats[variable] = {}
        
        for region_dir in variable_dir.iterdir():
            if not region_dir.is_dir():
                continue
                
            region = region_dir.name
            stats[variable][region] = {}
            
            for scenario_dir in region_dir.iterdir():
                if not scenario_dir.is_dir():
                    continue
                    
                scenario = scenario_dir.name
                csv_files = list(scenario_dir.glob("*.csv"))
                
                if csv_files:
                    file_count = len(csv_files)
                    file_sizes = sum(f.stat().st_size for f in csv_files)
                    total_files += file_count
                    total_size += file_sizes
                    
                    stats[variable][region][scenario] = {
                        'files': file_count,
                        'size_bytes': file_sizes,
                        'last_modified': max(f.stat().st_mtime for f in csv_files)
                    }
    
    return {
        'by_variable': stats,
        'total_files': total_files,
        'total_size_mb': total_size / (1024 * 1024),
        'last_update': time.time()
    }


def display_progress(stats, expected_total=20):
    """Display processing progress."""
    
    console.clear()
    
    # Header
    console.print(Panel.fit(
        "[bold cyan]Climate Data Processing Monitor[/bold cyan]\n"
        f"Monitoring batch processing of {expected_total} datasets",
        title="📊 Progress Monitor"
    ))
    
    # System resources
    memory = psutil.virtual_memory()
    cpu_percent = psutil.cpu_percent(interval=1)
    
    console.print(f"\n[bold]System Status:[/bold]")
    console.print(f"  CPU Usage: {cpu_percent:.1f}%")
    console.print(f"  Memory Usage: {memory.percent:.1f}% ({memory.used / (1024**3):.1f} GB / {memory.total / (1024**3):.1f} GB)")
    console.print(f"  Available Memory: {memory.available / (1024**3):.1f} GB")
    
    # Processing progress
    total_files = stats.get('total_files', 0)
    progress_pct = (total_files / expected_total) * 100 if expected_total > 0 else 0
    
    console.print(f"\n[bold]Processing Progress:[/bold]")
    console.print(f"  Completed: {total_files} / {expected_total} datasets ({progress_pct:.1f}%)")
    console.print(f"  Total output size: {stats.get('total_size_mb', 0):.1f} MB")
    
    if stats.get('last_update'):
        last_update = datetime.fromtimestamp(stats['last_update'])
        console.print(f"  Last update: {last_update.strftime('%H:%M:%S')}")
    
    # Progress bar
    bar_width = 50
    filled = int(bar_width * progress_pct / 100)
    bar = "█" * filled + "░" * (bar_width - filled)
    console.print(f"  Progress: [{bar}] {progress_pct:.1f}%")
    
    # Detailed breakdown
    if stats.get('by_variable'):
        table = Table(title="Completed Datasets")
        table.add_column("Variable", style="cyan")
        table.add_column("Region", style="magenta")
        table.add_column("Files", justify="right")
        table.add_column("Size (MB)", justify="right")
        table.add_column("Last Modified", style="dim")
        
        for variable, regions in stats['by_variable'].items():
            for region, scenarios in regions.items():
                for scenario, data in scenarios.items():
                    last_mod = datetime.fromtimestamp(data['last_modified'])
                    table.add_row(
                        variable.upper(),
                        region.title(),
                        str(data['files']),
                        f"{data['size_bytes'] / (1024*1024):.1f}",
                        last_mod.strftime('%H:%M:%S')
                    )
        
        if table.rows:
            console.print(f"\n")
            console.print(table)
    
    # Expected datasets
    expected_datasets = [
        "CONUS-TASMIN", "CONUS-TAS", "CONUS-TASMAX", "CONUS-PR",
        "ALASKA-PR", "ALASKA-TASMIN", "ALASKA-TAS", "ALASKA-TASMAX", 
        "HAWAII-PR", "HAWAII-TASMIN", "HAWAII-TAS", "HAWAII-TASMAX",
        "GUAM-PR", "GUAM-TAS", "GUAM-TASMIN", "GUAM-TASMAX",
        "PUERTO_RICO-PR", "PUERTO_RICO-TASMIN", "PUERTO_RICO-TAS", "PUERTO_RICO-TASMAX"
    ]
    
    completed_datasets = set()
    for variable, regions in stats.get('by_variable', {}).items():
        for region, scenarios in regions.items():
            for scenario in scenarios.keys():
                completed_datasets.add(f"{region.upper()}-{variable.upper()}")
    
    console.print(f"\n[bold]Remaining Datasets:[/bold]")
    remaining = [ds for ds in expected_datasets if ds not in completed_datasets]
    if remaining:
        for i, dataset in enumerate(remaining[:10]):  # Show first 10
            console.print(f"  {i+1}. {dataset}")
        if len(remaining) > 10:
            console.print(f"  ... and {len(remaining) - 10} more")
    else:
        console.print("  [green]All datasets completed![/green]")


def main():
    """Main monitoring loop."""
    
    stats_dir = Path("climate_outputs/stats")
    
    console.print("[bold green]Starting processing monitor...[/bold green]")
    console.print("[dim]Press Ctrl+C to stop monitoring[/dim]\n")
    
    try:
        while True:
            stats = get_processing_stats(stats_dir)
            display_progress(stats)
            
            # Check if all datasets are complete
            if stats.get('total_files', 0) >= 20:
                console.print("\n[bold green]🎉 All datasets completed![/bold green]")
                break
            
            time.sleep(10)  # Update every 10 seconds
            
    except KeyboardInterrupt:
        console.print("\n[yellow]Monitoring stopped by user[/yellow]")
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")


if __name__ == "__main__":
    main()