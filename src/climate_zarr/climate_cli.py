#!/usr/bin/env python
"""
🌡️ Climate Zarr CLI Tool - Modern 2025 Edition

A powerful CLI for processing climate data with NetCDF to Zarr conversion 
and county-level statistical analysis.

Features:
- Convert NetCDF files to optimized Zarr format
- Calculate detailed climate statistics by county/region
- Support for multiple climate variables (precipitation, temperature)
- Modern parallel processing with Rich progress bars
- Regional clipping with built-in boundary definitions
"""

import sys
from pathlib import Path
from typing import Optional, List
import warnings

import typer
import questionary
from rich import print as rprint
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.layout import Layout
from rich.columns import Columns
from rich.prompt import Prompt, Confirm
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from typing_extensions import Annotated

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=RuntimeWarning)

# Import our existing modules
from climate_zarr.stack_nc_to_zarr import stack_netcdf_to_zarr, stack_netcdf_to_zarr_hierarchical
from climate_zarr.county_processor import ModernCountyProcessor
from climate_zarr.utils.output_utils import get_output_manager
from climate_zarr.climate_config import get_config

# Initialize Rich console and Typer app
console = Console(highlight=False)
app = typer.Typer(
    name="climate-zarr",
    help="🌡️ Modern climate data processing toolkit",
    rich_markup_mode="rich",
    no_args_is_help=True,
)

# Configuration
CONFIG = get_config()


def interactive_region_selection() -> str:
    """Interactive region selection with descriptions."""
    choices = []
    for region_key, region_config in CONFIG.regions.items():
        description = f"{region_config.name} ({region_config.lat_min:.1f}°N to {region_config.lat_max:.1f}°N)"
        choices.append(questionary.Choice(title=description, value=region_key))
    
    return questionary.select(
        "🗺️ Select a region:",
        choices=choices,
        style=questionary.Style([
            ('question', 'bold blue'),
            ('answer', 'bold green'),
            ('pointer', 'bold yellow'),
            ('highlighted', 'bold cyan'),
        ])
    ).ask()


def interactive_variable_selection() -> str:
    """Interactive climate variable selection."""
    variables = {
        'pr': '🌧️ Precipitation (mm/day) - rainfall and snowfall',
        'tas': '🌡️ Air Temperature (°C) - daily mean temperature',
        'tasmax': '🔥 Daily Maximum Temperature (°C) - highest daily temp',
        'tasmin': '🧊 Daily Minimum Temperature (°C) - lowest daily temp'
    }
    
    choices = [
        questionary.Choice(title="🌟 All Variables - process all climate variables", value="all")
    ] + [
        questionary.Choice(title=description, value=var)
        for var, description in variables.items()
    ]
    
    return questionary.select(
        "🔬 Select climate variable(s) to analyze:",
        choices=choices,
        style=questionary.Style([
            ('question', 'bold blue'),
            ('answer', 'bold green'),
            ('pointer', 'bold yellow'),
            ('highlighted', 'bold cyan'),
        ])
    ).ask()


def interactive_file_selection() -> Path:
    """Interactive file/directory selection."""
    current_dir = Path.cwd()
    
    # Check for common data directories
    common_dirs = ['data', 'input', 'netcdf', 'nc_files']
    suggested_paths = []
    
    for dir_name in common_dirs:
        dir_path = current_dir / dir_name
        if dir_path.exists():
            nc_files = list(dir_path.glob("*.nc"))
            if nc_files:
                suggested_paths.append((dir_path, len(nc_files)))
    
    if suggested_paths:
        choices = []
        for path, count in suggested_paths:
            choices.append(
                questionary.Choice(
                    title=f"📁 {path.name}/ ({count} NetCDF files)", 
                    value=str(path)
                )
            )
        choices.append(questionary.Choice(title="📝 Enter custom path", value="custom"))
        
        selected = questionary.select(
            "📂 Select data source:",
            choices=choices,
            style=questionary.Style([
                ('question', 'bold blue'),
                ('answer', 'bold green'),
                ('pointer', 'bold yellow'),
                ('highlighted', 'bold cyan'),
            ])
        ).ask()
        
        if selected == "custom":
            return Path(questionary.path("Enter path to NetCDF files:").ask())
        else:
            return Path(selected)
    else:
        return Path(questionary.path("📂 Enter path to NetCDF files:").ask())


def confirm_operation(operation: str, details: dict) -> bool:
    """Confirm potentially destructive operations."""
    console.print(f"\n[yellow]⚠️ About to {operation}[/yellow]")
    
    # Show operation details
    details_table = Table(show_header=False, border_style="yellow")
    details_table.add_column("Setting", style="cyan")
    details_table.add_column("Value", style="white")
    
    for key, value in details.items():
        details_table.add_row(key, str(value))
    
    console.print(details_table)
    
    return questionary.confirm(
        f"🤔 Proceed with {operation}?",
        default=False,
        style=questionary.Style([
            ('question', 'bold yellow'),
            ('answer', 'bold green'),
        ])
    ).ask()


def print_banner():
    """Display a beautiful banner."""
    banner = Panel.fit(
        "[bold blue]🌡️ Climate Zarr Toolkit[/bold blue]\n"
        "[dim]Modern NetCDF → Zarr conversion & county statistics[/dim]",
        border_style="blue",
    )
    console.print(banner)


def validate_region(region: str) -> str:
    """Validate region name against available regions."""
    if region is None:
        return region
    
    available_regions = list(CONFIG.regions.keys())
    if region.lower() not in available_regions:
        rprint(f"[red]❌ Unknown region: {region}[/red]")
        rprint(f"[yellow]Available regions:[/yellow] {', '.join(available_regions)}")
        
        # Interactive suggestion
        if questionary.confirm("🤔 Would you like to select from available regions?").ask():
            return interactive_region_selection()
        else:
            raise typer.Exit(1)
    return region.lower()


def get_shapefile_for_region(region: str) -> Path:
    """Get the appropriate shapefile path for a region."""
    region_files = {
        'conus': 'conus_counties.shp',
        'alaska': 'alaska_counties.shp', 
        'hawaii': 'hawaii_counties.shp',
        'guam': 'guam_counties.shp',
        'puerto_rico': 'puerto_rico_counties.shp',
        'other': 'other_counties.shp'
    }
    
    shapefile_name = region_files.get(region, f'{region}_counties.shp')
    shapefile_path = Path('regional_counties') / shapefile_name
    
    if not shapefile_path.exists():
        rprint(f"[red]❌ Shapefile not found: {shapefile_path}[/red]")
        raise typer.Exit(1)
    
    return shapefile_path


@app.command("create-zarr")
def create_zarr(
    input_path: Annotated[Optional[Path], typer.Argument(help="Directory containing NetCDF files or single NetCDF file")] = None,
    output: Annotated[Optional[Path], typer.Option("--output", "-o", help="Output Zarr store path")] = None,
    region: Annotated[Optional[str], typer.Option("--region", "-r", help="Clip data to specific region")] = None,
    concat_dim: Annotated[str, typer.Option("--concat-dim", "-d", help="Dimension to concatenate along")] = "time",
    chunks: Annotated[Optional[str], typer.Option("--chunks", "-c", help="Chunk sizes as 'dim1=size1,dim2=size2'")] = None,
    compression: Annotated[str, typer.Option("--compression", help="Compression algorithm")] = "default",
    compression_level: Annotated[int, typer.Option("--compression-level", help="Compression level (1-9)")] = 5,
    interactive: Annotated[bool, typer.Option("--interactive", "-i", help="Use interactive prompts for missing options")] = True,
):
    """
    🗜️ Convert NetCDF files to optimized Zarr format.
    
    This command stacks multiple NetCDF files into a single, compressed Zarr store
    with optimal chunking for analysis workflows.
    
    Examples:
        climate-zarr create-zarr  # Interactive mode
        climate-zarr create-zarr data/ -o precipitation.zarr --region conus
        climate-zarr create-zarr data/ -o temp.zarr --chunks "time=365,lat=180,lon=360"
    """
    print_banner()
    
    # Interactive prompts for missing parameters
    if not input_path and interactive:
        input_path = interactive_file_selection()
    elif not input_path:
        rprint("[red]❌ Input path is required[/red]")
        raise typer.Exit(1)
    
    if not output and interactive:
        suggested = Path(f"{input_path.stem}_climate.zarr" if input_path.is_file() else "climate_data.zarr")
        output = Path(Prompt.ask("📁 Output Zarr file", default=str(suggested)))
    elif not output:
        output = Path("climate_data.zarr")
    
    if not region and interactive:
        if Confirm.ask("🗺️ Clip data to a specific region?"):
            region = interactive_region_selection()
    
    # Validate region if specified
    if region:
        region = validate_region(region)
    
    # Collect NetCDF files
    nc_files = []
    if input_path.is_dir():
        nc_files = list(input_path.glob("*.nc"))
    elif input_path.is_file() and input_path.suffix == '.nc':
        nc_files = [input_path]
    else:
        rprint(f"[red]❌ No NetCDF files found in: {input_path}[/red]")
        raise typer.Exit(1)
    
    if not nc_files:
        rprint(f"[red]❌ No .nc files found in directory: {input_path}[/red]")
        raise typer.Exit(1)
    
    # Parse chunks if provided
    chunks_dict = None
    if chunks:
        chunks_dict = {}
        for chunk in chunks.split(','):
            key, value = chunk.split('=')
            chunks_dict[key.strip()] = int(value.strip())
    
    # Confirmation for large datasets
    if len(nc_files) > 50 and interactive:
        if not Confirm.ask(f"⚠️ Process {len(nc_files)} files? This may take a while."):
            console.print("[yellow]❌ Operation cancelled[/yellow]")
            raise typer.Exit(0)
    
    # Display processing info
    info_table = Table(title="📊 Processing Configuration", show_header=False)
    info_table.add_column("Setting", style="cyan")
    info_table.add_column("Value", style="green")
    
    info_table.add_row("Input Files", f"{len(nc_files)} NetCDF files")
    info_table.add_row("Output", str(output))
    info_table.add_row("Region", region if region else "Global (no clipping)")
    info_table.add_row("Concat Dimension", concat_dim)
    info_table.add_row("Compression", f"{compression} (level {compression_level})")
    if chunks_dict:
        chunks_str = ", ".join(f"{k}={v}" for k, v in chunks_dict.items())
        info_table.add_row("Chunks", chunks_str)
    
    console.print(info_table)
    console.print()
    
    try:
        # Run the conversion
        stack_netcdf_to_zarr(
            nc_files=nc_files,
            zarr_path=output,
            concat_dim=concat_dim,
            chunks=chunks_dict,
            compression=compression,
            compression_level=compression_level,
            clip_region=region
        )
        
        # Success message
        success_panel = Panel(
            f"[green]✅ Successfully created Zarr store: {output}[/green]",
            border_style="green"
        )
        console.print(success_panel)
        
    except Exception as e:
        rprint(f"[red]❌ Error creating Zarr store: {e}[/red]")
        raise typer.Exit(1)


@app.command("county-stats")
def county_stats(
    zarr_path: Annotated[Optional[Path], typer.Argument(help="Path to Zarr dataset")] = None,
    region: Annotated[Optional[str], typer.Argument(help="Region name (conus, alaska, hawaii, etc.)")] = None,
    output: Annotated[Optional[Path], typer.Option("--output", "-o", help="Output CSV file")] = None,
    variable: Annotated[Optional[str], typer.Option("--variable", "-v", help="Climate variable to analyze")] = None,
    scenario: Annotated[str, typer.Option("--scenario", "-s", help="Scenario name")] = "historical",
    threshold: Annotated[Optional[float], typer.Option("--threshold", "-t", help="Threshold value")] = None,
    workers: Annotated[int, typer.Option("--workers", "-w", help="Number of worker processes")] = 4,
    use_distributed: Annotated[bool, typer.Option("--distributed", help="Use Dask distributed processing")] = False,
    interactive: Annotated[bool, typer.Option("--interactive", "-i", help="Use interactive prompts for missing options")] = True,
):
    """
    📈 Calculate detailed climate statistics by county for a specific region.
    
    Analyzes climate data and generates comprehensive statistics for each county
    in the specified region, with support for multiple climate variables.
    
    Examples:
        climate-zarr county-stats  # Interactive mode
        climate-zarr county-stats precipitation.zarr conus -v pr -t 25.4
        climate-zarr county-stats temperature.zarr alaska -v tas --workers 8
    """
    print_banner()
    
    # Interactive prompts for missing parameters
    if not zarr_path and interactive:
        zarr_path = Path(Prompt.ask("📁 Path to Zarr dataset"))
    elif not zarr_path:
        rprint("[red]❌ Zarr path is required[/red]")
        raise typer.Exit(1)
    
    if not zarr_path.exists():
        rprint(f"[red]❌ Zarr dataset not found: {zarr_path}[/red]")
        raise typer.Exit(1)
    
    if not region and interactive:
        region = interactive_region_selection()
    elif not region:
        rprint("[red]❌ Region is required[/red]")
        raise typer.Exit(1)
    
    region = validate_region(region)
    
    if not variable and interactive:
        variable = interactive_variable_selection()
    elif not variable:
        variable = "pr"
    shapefile_path = get_shapefile_for_region(region)
    
    # Variable validation
    valid_variables = ["pr", "tas", "tasmax", "tasmin"]
    if variable not in valid_variables:
        rprint(f"[red]❌ Invalid variable: {variable}[/red]")
        rprint(f"[yellow]Valid variables:[/yellow] {', '.join(valid_variables)}")
        raise typer.Exit(1)
    
    if threshold is None and interactive:
        default_threshold = "25.4" if variable == "pr" else "32" if variable == "tasmax" else "0"
        threshold_str = Prompt.ask(
            f"🎯 Threshold value ({'mm/day' if variable == 'pr' else '°C'})",
            default=default_threshold
        )
        threshold = float(threshold_str)
    elif threshold is None:
        threshold = 25.4 if variable == "pr" else 32.0 if variable == "tasmax" else 0.0
    
    if not output and interactive:
        # Use output manager to suggest standardized filename
        output_manager = get_output_manager()
        suggested_path = output_manager.get_output_path(
            variable=variable,
            region=region,
            scenario=scenario,
            threshold=threshold
        )
        output = Path(Prompt.ask(
            "📊 Output CSV file",
            default=str(suggested_path)
        ))
    elif not output:
        # Auto-generate standardized output path
        output_manager = get_output_manager()
        output = output_manager.get_output_path(
            variable=variable,
            region=region,
            scenario=scenario,
            threshold=threshold
        )
    
    # Confirmation for large operations
    if interactive and workers > 8:
        if not Confirm.ask(f"⚠️ Use {workers} workers? This will use significant system resources."):
            workers = 4
            console.print("[yellow]🔧 Reduced to 4 workers[/yellow]")
    
    # Display processing configuration
    config_table = Table(title="🔧 Analysis Configuration", show_header=False)
    config_table.add_column("Setting", style="cyan")
    config_table.add_column("Value", style="green")
    
    config_table.add_row("Zarr Dataset", str(zarr_path))
    config_table.add_row("Region", region.upper())
    config_table.add_row("Shapefile", str(shapefile_path))
    config_table.add_row("Variable", variable.upper())
    config_table.add_row("Scenario", scenario)
    config_table.add_row("Threshold", f"{threshold} {'mm' if variable == 'pr' else '°C'}")
    config_table.add_row("Workers", str(workers))
    config_table.add_row("Processing", "Distributed" if use_distributed else "Multiprocessing")
    config_table.add_row("Output", str(output))
    
    console.print(config_table)
    console.print()
    
    try:
        # Create processor
        processor = ModernCountyProcessor(
            n_workers=workers
        )
        
        # Load shapefile
        console.print("[blue]📍 Loading county boundaries...[/blue]")
        gdf = processor.prepare_shapefile(shapefile_path)
        
        # Process data
        console.print(f"[blue]🔄 Processing {variable.upper()} data for {len(gdf)} counties...[/blue]")
        results_df = processor.process_zarr_data(
            zarr_path=zarr_path,
            gdf=gdf,
            scenario=scenario,
            variable=variable,
            threshold=threshold
        )
        
        # Save results with metadata
        if 'output_manager' not in locals():
            output_manager = get_output_manager()
        
        metadata = {
            "processing_info": {
                "zarr_path": str(zarr_path),
                "shapefile_path": str(shapefile_path),
                "variable": variable,
                "scenario": scenario,
                "threshold": threshold,
                "workers": workers,
                "use_distributed": use_distributed
            },
            "data_summary": {
                "counties_processed": len(results_df['county_id'].unique()),
                "years_analyzed": len(results_df['year'].unique()),
                "total_records": len(results_df)
            }
        }
        
        output_manager.save_with_metadata(
            data=results_df,
            output_path=output,
            metadata=metadata,
            save_method="csv"
        )
        
        # Display summary
        summary_table = Table(title="📊 Processing Summary")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="magenta")
        
        summary_table.add_row("Counties Processed", str(len(results_df['county_id'].unique())))
        summary_table.add_row("Years Analyzed", str(len(results_df['year'].unique())))
        summary_table.add_row("Total Records", str(len(results_df)))
        summary_table.add_row("Variable", variable.upper())
        summary_table.add_row("Output File", str(output))
        
        console.print(summary_table)
        
        # Success message
        success_panel = Panel(
            f"[green]✅ County statistics saved to: {output}[/green]",
            border_style="green"
        )
        console.print(success_panel)
        
        # Clean up
        processor.close()
        
    except Exception as e:
        rprint(f"[red]❌ Error processing county statistics: {e}[/red]")
        raise typer.Exit(1)


@app.command("wizard")
def interactive_wizard():
    """
    🧙‍♂️ Launch the interactive wizard for guided climate data processing.
    
    This wizard will guide you through the entire process step-by-step,
    from selecting data to generating results.
    """
    print_banner()
    
    console.print(Panel(
        "[bold cyan]🧙‍♂️ Welcome to the Climate Data Processing Wizard![/bold cyan]\n\n"
        "This interactive guide will help you:\n"
        "• Convert NetCDF files to optimized Zarr format\n"
        "• Calculate detailed county statistics\n"
        "• Choose the best settings for your analysis\n\n"
        "[dim]Let's get started![/dim]",
        border_style="cyan"
    ))
    
    # Step 1: Choose operation
    operation = questionary.select(
        "🎯 What would you like to do?",
        choices=[
            questionary.Choice("🗜️ Convert NetCDF to Zarr", "convert"),
            questionary.Choice("📈 Calculate county statistics", "stats"),
            questionary.Choice("🔄 Full pipeline (convert + analyze)", "pipeline"),
            questionary.Choice("ℹ️ Just show me information", "info"),
        ],
        style=questionary.Style([
            ('question', 'bold blue'),
            ('answer', 'bold green'),
            ('pointer', 'bold yellow'),
            ('highlighted', 'bold cyan'),
        ])
    ).ask()
    
    if operation == "info":
        info()
        return
    
    # Step 2: Configure conversion settings (improved UX flow)
    nc_files = []
    if operation in ["convert", "pipeline"]:
        console.print("\n[bold blue]🗜️ Step 1: Configure Zarr conversion[/bold blue]")
        
        # Region selection (always required)
        region = interactive_region_selection()
        
        # Variable selection
        variable_selection = interactive_variable_selection()
        
        # Determine which variables to process
        all_variables = ['pr', 'tas', 'tasmax', 'tasmin']
        if variable_selection == "all":
            variables_to_process = all_variables
            console.print(f"[cyan]📊 Will convert all variables: {', '.join(variables_to_process)}[/cyan]")
        else:
            variables_to_process = [variable_selection]
        
        # Auto-detect NetCDF files based on variable selection
        console.print(f"\n[bold blue]📁 Step 2: Auto-detecting NetCDF data for {', '.join(variables_to_process)}[/bold blue]")
        
        nc_files = []
        auto_detected_vars = []
        missing_vars = []
        
        # Try to auto-detect files for each variable
        for variable in variables_to_process:
            # Common patterns to check
            possible_paths = [
                Path(f"data/{variable}/historical/"),
                Path(f"data/{variable}/"),
                Path("data/"),
            ]
            
            found_files = []
            for data_path in possible_paths:
                if data_path.exists():
                    # Look for files matching the variable
                    pattern_files = list(data_path.glob(f"{variable}_*.nc"))
                    if pattern_files:
                        found_files.extend(pattern_files)
                        break
                    # Also try generic pattern in case files don't start with variable name
                    pattern_files = list(data_path.glob("*.nc"))
                    if pattern_files and variable in str(pattern_files[0]).lower():
                        found_files.extend([f for f in pattern_files if variable in str(f).lower()])
                        break
            
            if found_files:
                nc_files.extend(found_files)
                auto_detected_vars.append(variable)
                console.print(f"[green]✅ Found {len(found_files)} files for {variable.upper()}: {found_files[0].parent}[/green]")
            else:
                missing_vars.append(variable)
                console.print(f"[yellow]⚠️ No files found for {variable.upper()}[/yellow]")
        
        # Handle missing variables
        if missing_vars:
            console.print(f"\n[yellow]📁 Manual selection needed for: {', '.join(missing_vars)}[/yellow]")
            manual_path = interactive_file_selection()
            
            if manual_path.exists():
                if manual_path.is_dir():
                    manual_files = list(manual_path.glob("*.nc"))
                elif manual_path.is_file() and manual_path.suffix == '.nc':
                    manual_files = [manual_path]
                else:
                    manual_files = []
                
                if manual_files:
                    nc_files.extend(manual_files)
                    console.print(f"[green]✅ Added {len(manual_files)} manual files[/green]")
        
        if not nc_files:
            console.print("[red]❌ No NetCDF files found. Please check your data directory structure.[/red]")
            console.print("[dim]Expected structure: data/{variable}/historical/{variable}_*.nc[/dim]")
            return
        
        # Remove duplicates while preserving order
        nc_files = list(dict.fromkeys(nc_files))
        
        # Show auto-detection summary
        if auto_detected_vars:
            console.print(f"\n[bold green]🎯 Auto-detected: {', '.join(auto_detected_vars).upper()}[/bold green]")
        if missing_vars:
            console.print(f"[yellow]📝 Manual selection: {', '.join(missing_vars).upper()}[/yellow]")
        console.print(f"[bold green]✅ Total: {len(nc_files)} NetCDF files ready for conversion[/bold green]")
        
        # Use recommended compression by default
        compression = "zstd"
        console.print("[green]🗜️ Using ZSTD compression (recommended - fast & efficient)[/green]")
        
        # Confirm conversion
        variables_display = "All Variables" if variable_selection == "all" else variable_selection.upper()
        conversion_details = {
            "Input Files": f"{len(nc_files)} NetCDF files",
            "Variables": variables_display,
            "Output Structure": "Hierarchical (climate_outputs/zarr/{variable}/{region}/historical/)",
            "Region": region.upper(),
            "Compression": compression,
        }
        
        if not confirm_operation("convert NetCDF to Zarr", conversion_details):
            console.print("[yellow]❌ Operation cancelled by user[/yellow]")
            return
        
        # Perform conversion - create separate zarr files for each variable
        try:
            console.print(f"\n[blue]🔄 Converting {variables_display} NetCDF files to hierarchical Zarr format...[/blue]")
            
            created_zarr_paths = []
            
            # Group files by variable
            files_by_variable = {}
            for variable in variables_to_process:
                variable_files = [f for f in nc_files if variable in str(f).lower()]
                if variable_files:
                    files_by_variable[variable] = variable_files
            
            # Convert each variable separately
            for variable, variable_files in files_by_variable.items():
                console.print(f"\n[cyan]📦 Processing {variable.upper()} ({len(variable_files)} files)...[/cyan]")
                
                # Use hierarchical conversion
                zarr_path = stack_netcdf_to_zarr_hierarchical(
                    nc_files=variable_files,
                    variable=variable,
                    region=region,
                    scenario="historical",
                    concat_dim="time",
                    chunks=None,
                    compression=compression,
                    compression_level=5
                )
                
                created_zarr_paths.append(zarr_path)
                console.print(f"[green]✅ {variable.upper()} complete: {zarr_path}[/green]")
            
            # Show summary of created files
            console.print(Panel(
                f"[green]✅ Successfully created {len(created_zarr_paths)} hierarchical Zarr stores:[/green]\n" +
                "\n".join([f"📁 {path}" for path in created_zarr_paths]),
                border_style="green"
            ))
            
            # Set output_path for pipeline mode (use first created path as representative)
            if created_zarr_paths:
                output_path = created_zarr_paths[0]
            
        except Exception as e:
            console.print(f"[red]❌ Error during conversion: {e}[/red]")
            return
    
    if operation in ["stats", "pipeline"]:
        # Step 3 or 4: Statistics configuration (depending on whether conversion was done)
        step_num = "Step 1" if operation == "stats" else "Step 3"
        console.print(f"\n[bold blue]📈 {step_num}: Configure county statistics[/bold blue]")
        
        # For stats-only operation, ask for region and variable
        # For pipeline mode, reuse selections from conversion step
        if operation == "stats":
            # Region for statistics
            stats_region = interactive_region_selection()
            
            # Variable selection
            variable_selection = interactive_variable_selection()
            
            # Determine which variables to process
            all_variables = ['pr', 'tas', 'tasmax', 'tasmin']
            if variable_selection == "all":
                variables_to_process = all_variables
                console.print(f"[cyan]📊 Will process all variables: {', '.join(variables_to_process)}[/cyan]")
            else:
                variables_to_process = [variable_selection]
        else:
            # Pipeline mode: reuse region and variable selections from conversion
            stats_region = region
            # variables_to_process and variable_selection already set from conversion step
            console.print(f"[cyan]📊 Processing statistics for: {', '.join(variables_to_process)}[/cyan]")
        
        # For pipeline mode, we already have the zarr_path
        if operation != "stats":
            zarr_path = output_path
        
        # Performance settings
        workers = questionary.select(
            "⚡ Number of worker processes:",
            choices=["2", "4", "8", "16", "32"],
            default="4"
        ).ask()
        
        # Note: Distributed processing not currently implemented in ModernCountyProcessor
        use_distributed = False
        
        # Confirm statistics calculation
        variables_display = "All Variables" if variable_selection == "all" else variable_selection.upper()
        stats_details = {
            "Region": stats_region.upper(),
            "Variables": variables_display,
            "Workers": workers,
            "Processing": "Distributed" if use_distributed else "Multiprocessing",
        }
        
        if not confirm_operation("calculate county statistics", stats_details):
            console.print("[yellow]❌ Operation cancelled by user[/yellow]")
            return
        
        # Perform statistics calculation
        try:
            # Get shapefile path
            shapefile_path = get_shapefile_for_region(stats_region)
            
            # Create processor
            processor = ModernCountyProcessor(
                n_workers=int(workers)
            )
            
            # Load shapefile once
            console.print("[blue]📍 Loading county boundaries...[/blue]")
            gdf = processor.prepare_shapefile(shapefile_path)
            
            # Get output manager once
            output_manager = get_output_manager()
            processed_files = []
            
            # Process each variable
            for variable in variables_to_process:
                console.print(f"\n[bold cyan]🔄 Processing {variable.upper()} data for {len(gdf)} counties...[/bold cyan]")
                
                # Find or verify zarr path for this variable
                if operation == "stats":
                    # Auto-find zarr path for stats-only mode using hierarchical structure
                    expected_zarr = Path(f"climate_outputs/zarr/{variable}/{stats_region}/historical/{stats_region}_historical_{variable}.zarr")
                    if expected_zarr.exists():
                        current_zarr_path = expected_zarr
                        console.print(f"[green]✅ Found Zarr: {current_zarr_path}[/green]")
                    else:
                        console.print(f"[yellow]⚠️ Skipping {variable}: Zarr not found at {expected_zarr}[/yellow]")
                        continue
                else:
                    # Pipeline mode - find the zarr for this specific variable from created paths
                    variable_zarr = None
                    for created_path in created_zarr_paths:
                        if f"/{variable}/" in str(created_path) and f"_{variable}.zarr" in str(created_path):
                            variable_zarr = created_path
                            break
                    
                    if variable_zarr:
                        current_zarr_path = variable_zarr
                    else:
                        console.print(f"[yellow]⚠️ Skipping {variable}: Could not find corresponding zarr file[/yellow]")
                        continue
                
                # Get default threshold for this variable
                if variable == "pr":
                    threshold = 25.4
                elif variable == "tasmax":
                    threshold = 32.0
                elif variable == "tasmin":
                    threshold = 0.0
                else:
                    threshold = 0.0
                
                # Process data
                results_df = processor.process_zarr_data(
                    zarr_path=current_zarr_path,
                    gdf=gdf,
                    scenario="historical",
                    variable=variable,
                    threshold=threshold
                )
                
                # Generate output path
                suggested_output = output_manager.get_output_path(
                    variable=variable,
                    region=stats_region,
                    scenario="historical",
                    threshold=threshold
                )
                
                # Save results with metadata
                metadata = {
                    "processing_info": {
                        "zarr_path": str(current_zarr_path),
                        "shapefile_path": str(shapefile_path),
                        "variable": variable,
                        "scenario": "historical",
                        "threshold": threshold,
                        "workers": int(workers),
                        "use_distributed": use_distributed
                    },
                    "data_summary": {
                        "counties_processed": len(results_df['county_id'].unique()),
                        "years_analyzed": len(results_df['year'].unique()),
                        "total_records": len(results_df)
                    }
                }
                
                output_manager.save_with_metadata(
                    data=results_df,
                    output_path=suggested_output,
                    metadata=metadata,
                    save_method="csv"
                )
                
                # Get full absolute path for display
                full_csv_path = suggested_output.resolve()
                
                processed_files.append({
                    'variable': variable,
                    'output': str(full_csv_path),
                    'counties': len(results_df['county_id'].unique()),
                    'records': len(results_df)
                })
                
                console.print(f"[green]✅ {variable.upper()} complete: {full_csv_path}[/green]")
            
            # Show comprehensive summary
            if processed_files:
                summary_table = Table(title="📊 Processing Complete!")
                summary_table.add_column("Variable", style="cyan")
                summary_table.add_column("Counties", style="yellow")
                summary_table.add_column("Records", style="magenta")
                summary_table.add_column("Output File", style="green")
                
                for file_info in processed_files:
                    summary_table.add_row(
                        file_info['variable'].upper(),
                        str(file_info['counties']),
                        str(file_info['records']),
                        file_info['output']
                    )
                
                console.print(summary_table)
                
                # Success message
                variables_processed = [f['variable'].upper() for f in processed_files]
                console.print(Panel(
                    f"[green]✅ Successfully processed {len(processed_files)} variable(s): {', '.join(variables_processed)}[/green]",
                    border_style="green"
                ))
            else:
                console.print("[yellow]⚠️ No variables were processed[/yellow]")
            
            # Clean up
            processor.close()
            
        except Exception as e:
            console.print(f"[red]❌ Error processing statistics: {e}[/red]")
            return
    
    # Final success message
    console.print("\n" + "🎉" * 50)
    console.print(Panel(
        "[bold green]🎊 Wizard completed successfully![/bold green]\n\n"
        "[cyan]What you accomplished:[/cyan]\n"
        f"• {'✅ Converted NetCDF to Zarr format' if operation in ['convert', 'pipeline'] else ''}\n"
        f"• {'✅ Calculated detailed county statistics' if operation in ['stats', 'pipeline'] else ''}\n"
        f"• {'✅ Processed ' + str(len(nc_files)) + ' NetCDF files' if nc_files else ''}\n\n"
        "[dim]🚀 You're ready to explore your climate data![/dim]",
        border_style="green",
        title="🏆 Success"
    ))


@app.command("interactive")  
def interactive_mode():
    """
    🎮 Enter interactive mode for guided climate data processing.
    
    This launches an interactive session where you can explore data,
    run commands, and get guided assistance.
    """
    interactive_wizard()


@app.command("list-regions")
def list_regions():
    """📍 List all available regions for clipping and analysis."""
    print_banner()
    
    regions_table = Table(title="🗺️ Available Regions")
    regions_table.add_column("Region", style="cyan")
    regions_table.add_column("Name", style="green")
    regions_table.add_column("Boundaries (Lat/Lon)", style="yellow")
    
    for region_key, region_config in CONFIG.regions.items():
        bounds = f"{region_config.lat_min:.1f}°N to {region_config.lat_max:.1f}°N, "
        bounds += f"{region_config.lon_min:.1f}°E to {region_config.lon_max:.1f}°E"
        
        regions_table.add_row(
            region_key,
            region_config.name,
            bounds
        )
    
    console.print(regions_table)


@app.command("info")
def info():
    """ℹ️ Display system information and available data."""
    print_banner()
    
    # Check data directory
    data_dir = Path("data")
    nc_files = list(data_dir.glob("*.nc")) if data_dir.exists() else []
    
    # Check regional counties
    regional_dir = Path("regional_counties")
    shapefiles = list(regional_dir.glob("*.shp")) if regional_dir.exists() else []
    
    # System info
    info_layout = Layout()
    info_layout.split_column(
        Layout(name="data"),
        Layout(name="regions")
    )
    
    # Data info
    data_table = Table(title="📁 Available Data")
    data_table.add_column("Type", style="cyan")
    data_table.add_column("Count", style="green")
    data_table.add_column("Location", style="yellow")
    
    data_table.add_row("NetCDF Files", str(len(nc_files)), str(data_dir))
    data_table.add_row("Regional Shapefiles", str(len(shapefiles)), str(regional_dir))
    
    # Regions info
    regions_table = Table(title="🗺️ Configured Regions")
    regions_table.add_column("Region", style="cyan")
    regions_table.add_column("Coverage", style="green")
    
    for region_key, region_config in CONFIG.regions.items():
        regions_table.add_row(region_key, region_config.name)
    
    info_layout["data"].update(Panel(data_table, border_style="blue"))
    info_layout["regions"].update(Panel(regions_table, border_style="green"))
    
    console.print(info_layout)
    
    # Sample NetCDF files
    if nc_files:
        console.print(f"\n[dim]Sample NetCDF files (showing first 5):[/dim]")
        for nc_file in nc_files[:5]:
            console.print(f"  • {nc_file.name}")
        if len(nc_files) > 5:
            console.print(f"  ... and {len(nc_files) - 5} more")


if __name__ == "__main__":
    app() 