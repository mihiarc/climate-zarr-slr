#!/usr/bin/env python
"""
Comprehensive batch processing script for generating county-level climate statistics
from all processed Zarr datasets.

This script processes 20 Zarr datasets across:
- 4 variables: pr, tas, tasmax, tasmin
- 5 regions: CONUS, Alaska, Hawaii, Puerto Rico, Guam
- 1 scenario: ssp370

Features:
- Automatic strategy selection based on dataset size
- Progress tracking and memory monitoring
- Error handling and retry logic
- Comprehensive logging and reporting
"""

import sys
import time
import psutil
from pathlib import Path
from typing import Dict, List, Tuple
import numpy as np
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
)

# Add src to Python path for imports
# Script is in scripts/ directory, need to go up to project root first
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from climate_zarr.county_processor import ModernCountyProcessor

# Strategy selection is now handled internally by processors via create_processing_plan
from climate_zarr.processors.strategy_config import ProcessingConfig
from climate_zarr.utils.memory_utils import MemoryMonitor

console = Console()


class BatchCountyProcessor:
    """Batch processor for county statistics across multiple Zarr datasets."""

    def __init__(
        self,
        base_dir: Path = None,
        n_workers: int = 4,
        use_chunked_strategy: bool = True,
    ):
        """Initialize batch processor.

        Args:
            base_dir: Base directory containing climate_outputs/
            n_workers: Number of worker processes
            use_chunked_strategy: Whether to use the new optimized chunked strategy
        """
        self.base_dir = base_dir or Path.cwd()
        self.zarr_dir = self.base_dir / "climate_outputs" / "zarr"
        self.shapefile_dir = self.base_dir / "regional_counties"
        self.n_workers = n_workers
        self.use_chunked_strategy = use_chunked_strategy

        # Dataset configuration
        self.variables = ["pr", "tas", "tasmax", "tasmin"]
        self.regions = ["conus", "alaska", "hawaii", "puerto_rico", "guam"]
        self.scenario = "ssp370"

        # Memory configuration for optimized processing
        self.memory_monitor = MemoryMonitor()
        self.target_memory_usage = 0.75  # Use 75% of available memory

        # Processing configuration
        self.processing_config = ProcessingConfig(
            target_memory_usage=self.target_memory_usage,
            parallel_workers=n_workers,
            enable_spatial_cache=True,
            enable_adaptive_chunking=True,
            large_dataset_threshold_gb=10.0,  # Use chunked for >10GB
            many_counties_threshold=500,  # Use chunked for >500 counties
        )

        self.processing_log = []
        self.failed_datasets = []

    def discover_datasets(self) -> List[Dict]:
        """Discover all available Zarr datasets.

        Returns:
            List of dataset metadata dictionaries
        """
        datasets = []

        for variable in self.variables:
            for region in self.regions:
                zarr_path = (
                    self.zarr_dir
                    / variable
                    / region
                    / self.scenario
                    / f"{region}_{self.scenario}_{variable}_daily.zarr"
                )

                if zarr_path.exists():
                    # Get dataset size
                    size_bytes = sum(
                        f.stat().st_size for f in zarr_path.rglob("*") if f.is_file()
                    )
                    size_gb = size_bytes / (1024**3)

                    # Get shapefile path
                    shapefile_path = self.shapefile_dir / f"{region}_counties.shp"

                    datasets.append(
                        {
                            "variable": variable,
                            "region": region,
                            "scenario": self.scenario,
                            "zarr_path": zarr_path,
                            "shapefile_path": shapefile_path,
                            "size_gb": size_gb,
                            "size_bytes": size_bytes,
                            "strategy": self._select_strategy(size_gb, region),
                        }
                    )

        return sorted(datasets, key=lambda x: x["size_gb"], reverse=True)

    def _select_strategy(self, size_gb: float, region: str) -> str:
        """Select processing strategy based on dataset characteristics.

        Args:
            size_gb: Dataset size in GB
            region: Region name

        Returns:
            Strategy name ('SpatialChunked' or 'Vectorized')
        """
        if not self.use_chunked_strategy:
            # Force Vectorized if chunked strategy is disabled
            return "Vectorized"

        # Get county count for this region
        county_counts = {
            "conus": 3109,
            "alaska": 29,
            "hawaii": 5,
            "puerto_rico": 78,
            "guam": 1,
        }
        num_counties = county_counts.get(region, 100)

        # Use SpatialChunked strategy for optimal performance when:
        # - Large datasets (>10GB)
        # - Many counties (>500)
        # - CONUS region (always benefits from chunking)
        # - Sufficient memory available (>16GB)

        available_memory_gb = psutil.virtual_memory().available / (1024**3)

        if region == "conus":
            # Always use chunked for CONUS (3,109 counties)
            return "SpatialChunked"
        elif size_gb >= self.processing_config.large_dataset_threshold_gb:
            # Large datasets benefit from chunking
            return "SpatialChunked"
        elif num_counties >= self.processing_config.many_counties_threshold:
            # Many counties benefit from parallel processing
            return "SpatialChunked"
        elif available_memory_gb > 16 and size_gb > 5:
            # With sufficient memory, use chunking for better performance
            return "SpatialChunked"
        else:
            # Small datasets or limited memory - use simpler strategy
            return "Vectorized"

    def _get_variable_thresholds(self) -> Dict[str, float]:
        """Get default thresholds for each variable.

        Returns:
            Dictionary mapping variable names to threshold values
        """
        return {
            "pr": 25.4,  # 25.4mm heavy precipitation threshold
            "tas": 0.0,  # 0°C freezing point (not used in calculation)
            "tasmax": 35.0,  # 35°C extreme heat threshold
            "tasmin": 0.0,  # 0°C freezing threshold (not used in calculation)
        }

    def process_dataset(self, dataset_info: Dict) -> Tuple[bool, str, Dict]:
        """Process a single dataset.

        Args:
            dataset_info: Dataset metadata dictionary

        Returns:
            Tuple of (success, message, processing_stats)
        """
        variable = dataset_info["variable"]
        region = dataset_info["region"]
        zarr_path = dataset_info["zarr_path"]
        shapefile_path = dataset_info["shapefile_path"]
        strategy_name = dataset_info["strategy"]

        console.print(
            f"\n[bold blue]Processing {region.upper()} - {variable.upper()}[/bold blue]"
        )
        console.print(f"[dim]Dataset: {zarr_path}[/dim]")
        console.print(f"[dim]Size: {dataset_info['size_gb']:.2f} GB[/dim]")
        console.print(f"[dim]Strategy: {strategy_name}[/dim]")

        if not shapefile_path.exists():
            return False, f"Shapefile not found: {shapefile_path}", {}

        start_time = time.time()
        initial_memory = psutil.virtual_memory().used / (1024**3)

        try:
            # Initialize processor
            with ModernCountyProcessor(n_workers=self.n_workers) as processor:
                # Load shapefile
                console.print("[yellow]Loading county shapefile...[/yellow]")
                gdf = processor.prepare_shapefile(shapefile_path)
                console.print(f"[green]Loaded {len(gdf)} counties[/green]")

                # Open the Zarr dataset to get data characteristics
                import xarray as xr

                ds = xr.open_zarr(zarr_path, chunks={"time": 365})
                data = ds[variable]

                # The processors now handle strategy selection internally via create_processing_plan
                # We just need to display what strategy will be used
                if strategy_name == "SpatialChunked":
                    console.print(
                        "[cyan]Strategy: SpatialChunked (parallel processing)[/cyan]"
                    )
                    console.print(
                        f"[cyan]Memory target: {self.target_memory_usage * 100:.0f}% utilization[/cyan]"
                    )
                    console.print(f"[cyan]Workers: {self.n_workers}[/cyan]")
                else:
                    console.print(
                        "[cyan]Strategy: Vectorized (sequential processing)[/cyan]"
                    )

                # Get the processor for this variable
                var_processor = processor.get_processor(variable)

                # Monitor memory usage
                # Note: MemoryMonitor provides get_memory_status() and check_memory_pressure() methods
                # No need to explicitly start monitoring as it checks in real-time

                # Get threshold for this variable
                thresholds = self._get_variable_thresholds()
                threshold = thresholds.get(variable, 0.0)

                # Process the dataset
                console.print(
                    f"[yellow]Processing {variable} data with {strategy_name} strategy...[/yellow]"
                )

                # Show memory status
                mem_info = psutil.virtual_memory()
                console.print(
                    f"[dim]Memory available: {mem_info.available / (1024**3):.1f}GB / {mem_info.total / (1024**3):.1f}GB[/dim]"
                )

                results_df = processor.process_zarr_data(
                    zarr_path=zarr_path,
                    gdf=gdf,
                    scenario=self.scenario,
                    variable=variable,
                    threshold=threshold,
                )

                # Get memory stats
                if self.use_chunked_strategy:
                    memory_stats = self.memory_monitor.get_memory_status()
                    if memory_stats:
                        console.print(
                            f"[dim]Peak memory usage: {memory_stats.get('used_gb', 0):.2f} GB[/dim]"
                        )

                if results_df.empty:
                    return False, "No data processed - empty results", {}

                # Save results
                console.print("[yellow]Saving results...[/yellow]")
                output_path = processor.save_results(
                    results_df=results_df,
                    variable=variable,
                    region=region,
                    scenario=self.scenario,
                    threshold=threshold,
                )

                # Calculate processing statistics
                end_time = time.time()
                processing_time = end_time - start_time
                peak_memory = psutil.virtual_memory().used / (1024**3)
                memory_used = peak_memory - initial_memory

                # Get memory stats if available
                peak_memory_actual = memory_used
                if (
                    self.use_chunked_strategy
                    and "memory_stats" in locals()
                    and memory_stats
                ):
                    peak_memory_actual = memory_stats.get("peak_memory_gb", memory_used)

                processing_stats = {
                    "processing_time_seconds": processing_time,
                    "memory_used_gb": memory_used,
                    "peak_memory_gb": peak_memory_actual,
                    "records_processed": len(results_df),
                    "counties_processed": len(results_df["county_id"].unique())
                    if "county_id" in results_df.columns
                    else 0,
                    "years_processed": len(results_df["year"].unique())
                    if "year" in results_df.columns
                    else 0,
                    "output_path": str(output_path),
                    "strategy_used": strategy_name,
                    "throughput_counties_per_minute": (
                        len(results_df["county_id"].unique()) / processing_time * 60
                    )
                    if processing_time > 0
                    else 0,
                }

                console.print(
                    f"[green]✓ Successfully processed {len(results_df):,} records[/green]"
                )
                console.print(f"[green]  Output: {output_path}[/green]")
                console.print(
                    f"[dim]  Time: {processing_time:.1f}s, Memory: {memory_used:.2f} GB[/dim]"
                )
                if "throughput_counties_per_minute" in processing_stats:
                    console.print(
                        f"[dim]  Throughput: {processing_stats['throughput_counties_per_minute']:.1f} counties/min[/dim]"
                    )

                return True, "Success", processing_stats

        except Exception as e:
            error_msg = f"Processing failed: {str(e)}"
            console.print(f"[red]✗ {error_msg}[/red]")
            return False, error_msg, {}

    def generate_summary_report(self, datasets: List[Dict]) -> None:
        """Generate a comprehensive processing summary report.

        Args:
            datasets: List of dataset metadata
        """
        console.print("\n" + "=" * 80)
        console.print("[bold cyan]BATCH PROCESSING SUMMARY REPORT[/bold cyan]")
        console.print("=" * 80)

        # Overall statistics
        total_datasets = len(datasets)
        successful = len([log for log in self.processing_log if log["success"]])
        failed = len(self.failed_datasets)

        console.print("\n[bold]Overall Statistics:[/bold]")
        console.print(f"  Total datasets: {total_datasets}")
        console.print(f"  Successfully processed: {successful}")
        console.print(f"  Failed: {failed}")
        console.print(f"  Success rate: {(successful / total_datasets) * 100:.1f}%")

        if self.processing_log:
            total_time = sum(
                log["stats"].get("processing_time_seconds", 0)
                for log in self.processing_log
                if log["success"]
            )
            total_records = sum(
                log["stats"].get("records_processed", 0)
                for log in self.processing_log
                if log["success"]
            )
            avg_memory = np.mean(
                [
                    log["stats"].get("memory_used_gb", 0)
                    for log in self.processing_log
                    if log["success"]
                ]
            )

            console.print(
                f"  Total processing time: {total_time:.1f} seconds ({total_time / 60:.1f} minutes)"
            )
            console.print(f"  Total records processed: {total_records:,}")
            console.print(f"  Average memory usage: {avg_memory:.2f} GB")

        # Strategy usage and performance
        if self.processing_log:
            strategy_counts = {}
            strategy_performance = {}
            for log in self.processing_log:
                if log["success"]:
                    strategy = log["stats"].get("strategy_used", "Unknown")
                    strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

                    # Track performance metrics per strategy
                    if strategy not in strategy_performance:
                        strategy_performance[strategy] = {
                            "total_time": 0,
                            "total_counties": 0,
                            "peak_memory": [],
                        }
                    strategy_performance[strategy]["total_time"] += log["stats"].get(
                        "processing_time_seconds", 0
                    )
                    strategy_performance[strategy]["total_counties"] += log[
                        "stats"
                    ].get("counties_processed", 0)
                    strategy_performance[strategy]["peak_memory"].append(
                        log["stats"].get("peak_memory_gb", 0)
                    )

            console.print("\n[bold]Strategy Usage & Performance:[/bold]")
            for strategy, count in strategy_counts.items():
                perf = strategy_performance[strategy]
                avg_throughput = (
                    (perf["total_counties"] / perf["total_time"] * 60)
                    if perf["total_time"] > 0
                    else 0
                )
                avg_memory = np.mean(perf["peak_memory"]) if perf["peak_memory"] else 0
                console.print(f"  {strategy}: {count} datasets")
                console.print(
                    f"    - Avg throughput: {avg_throughput:.1f} counties/min"
                )
                console.print(f"    - Avg peak memory: {avg_memory:.1f} GB")

        # Dataset size breakdown
        size_ranges = {"Small (<100MB)": 0, "Medium (100MB-1GB)": 0, "Large (>1GB)": 0}
        for dataset in datasets:
            size_gb = dataset["size_gb"]
            if size_gb < 0.1:
                size_ranges["Small (<100MB)"] += 1
            elif size_gb < 1.0:
                size_ranges["Medium (100MB-1GB)"] += 1
            else:
                size_ranges["Large (>1GB)"] += 1

        console.print("\n[bold]Dataset Size Distribution:[/bold]")
        for size_range, count in size_ranges.items():
            console.print(f"  {size_range}: {count} datasets")

        # Failed datasets
        if self.failed_datasets:
            console.print("\n[bold red]Failed Datasets:[/bold red]")
            for failed in self.failed_datasets:
                console.print(
                    f"  [red]✗[/red] {failed['region']}-{failed['variable']}: {failed['error']}"
                )

        # Processing table
        if self.processing_log:
            table = Table(title="Detailed Processing Results")
            table.add_column("Region", style="cyan")
            table.add_column("Variable", style="magenta")
            table.add_column("Size (GB)", justify="right")
            table.add_column("Strategy", style="yellow")
            table.add_column("Status", style="green")
            table.add_column("Records", justify="right")
            table.add_column("Time (s)", justify="right")

            for log in self.processing_log:
                status = "✓" if log["success"] else "✗"
                status_style = "green" if log["success"] else "red"

                if log["success"]:
                    records = f"{log['stats'].get('records_processed', 0):,}"
                    proc_time = f"{log['stats'].get('processing_time_seconds', 0):.1f}"
                    strategy = log["stats"].get("strategy_used", "N/A")
                else:
                    records = "0"
                    proc_time = "N/A"
                    strategy = "N/A"

                table.add_row(
                    log["region"].title(),
                    log["variable"].upper(),
                    f"{log['size_gb']:.2f}",
                    strategy,
                    f"[{status_style}]{status}[/{status_style}]",
                    records,
                    proc_time,
                )

            console.print("\n")
            console.print(table)

        console.print("\n" + "=" * 80)

    def run_batch_processing(self) -> None:
        """Execute batch processing for all datasets."""

        console.print(
            Panel.fit(
                "[bold cyan]Climate Zarr County Statistics Batch Processor[/bold cyan]\n"
                "Processing 20 Zarr datasets across 4 variables and 5 regions",
                title="🌡️ Climate Data Processing",
            )
        )

        # Check system resources
        memory_info = psutil.virtual_memory()
        console.print("\n[bold]System Resources:[/bold]")
        console.print(f"  Available memory: {memory_info.available / (1024**3):.2f} GB")
        console.print(f"  Total memory: {memory_info.total / (1024**3):.2f} GB")
        console.print(f"  CPU cores: {psutil.cpu_count()}")
        console.print(f"  Workers: {self.n_workers}")

        # Discover datasets
        console.print("\n[yellow]Discovering Zarr datasets...[/yellow]")
        datasets = self.discover_datasets()

        if not datasets:
            console.print("[red]No Zarr datasets found![/red]")
            return

        console.print(f"[green]Found {len(datasets)} datasets to process[/green]")

        # Display processing plan
        plan_table = Table(title="Processing Plan")
        plan_table.add_column("Region", style="cyan")
        plan_table.add_column("Variable", style="magenta")
        plan_table.add_column("Size", justify="right")
        plan_table.add_column("Strategy", style="yellow")

        total_size = 0
        for dataset in datasets:
            plan_table.add_row(
                dataset["region"].title(),
                dataset["variable"].upper(),
                f"{dataset['size_gb']:.2f} GB",
                dataset["strategy"],
            )
            total_size += dataset["size_gb"]

        console.print("\n")
        console.print(plan_table)
        console.print(f"\n[bold]Total size to process: {total_size:.2f} GB[/bold]")

        # Process each dataset
        console.print("\n[bold green]Starting batch processing...[/bold green]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            main_task = progress.add_task("Processing datasets...", total=len(datasets))

            for i, dataset_info in enumerate(datasets):
                progress.update(
                    main_task,
                    description=f"Processing {dataset_info['region']}-{dataset_info['variable']} ({i + 1}/{len(datasets)})",
                )

                success, message, stats = self.process_dataset(dataset_info)

                # Log the result
                log_entry = {
                    "region": dataset_info["region"],
                    "variable": dataset_info["variable"],
                    "size_gb": dataset_info["size_gb"],
                    "success": success,
                    "message": message,
                    "stats": stats,
                }
                self.processing_log.append(log_entry)

                if not success:
                    self.failed_datasets.append(
                        {
                            "region": dataset_info["region"],
                            "variable": dataset_info["variable"],
                            "error": message,
                        }
                    )

                progress.advance(main_task)

        # Generate summary report
        self.generate_summary_report(datasets)


def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Batch process county statistics for climate data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Strategy Selection:
  The script automatically selects the optimal processing strategy:
  - SpatialChunked: For CONUS, large datasets (>10GB), or many counties (>500)
  - Vectorized: For small regions and datasets
  
  Use --no-chunking to force the simpler Vectorized strategy.
  
Performance Expectations:
  With SpatialChunked strategy (default for large datasets):
  - CONUS (3,109 counties): ~3-6 hours (vs 8-15 hours with Vectorized)
  - Memory usage: 70-80% of available RAM
  - Throughput: 15-20+ counties/minute
        """,
    )

    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=4,
        help="Number of worker processes (default: 4)",
    )

    parser.add_argument(
        "--no-chunking",
        action="store_true",
        help="Disable the optimized SpatialChunked strategy, use simple Vectorized instead",
    )

    parser.add_argument(
        "--memory-target",
        type=float,
        default=0.75,
        help="Target memory usage as fraction (default: 0.75 = 75%%)",
    )

    args = parser.parse_args()

    # Initialize batch processor with configuration
    processor = BatchCountyProcessor(
        n_workers=args.workers, use_chunked_strategy=not args.no_chunking
    )

    # Update memory target if specified
    if args.memory_target != 0.75:
        processor.target_memory_usage = args.memory_target
        processor.processing_config.target_memory_usage = args.memory_target
        console.print(
            f"[yellow]Memory target set to {args.memory_target * 100:.0f}%[/yellow]"
        )

    # Display configuration
    console.print(
        Panel.fit(
            f"[bold cyan]Batch Processing Configuration[/bold cyan]\n"
            f"Workers: {args.workers}\n"
            f"Strategy: {'SpatialChunked (optimized)' if not args.no_chunking else 'Vectorized (simple)'}\n"
            f"Memory Target: {processor.target_memory_usage * 100:.0f}%",
            title="⚙️ Settings",
        )
    )

    # Run batch processing
    processor.run_batch_processing()


if __name__ == "__main__":
    main()
