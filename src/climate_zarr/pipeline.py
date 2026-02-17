"""Core pipeline: NetCDF -> Zarr -> county-level stats CSV.

Provides a single ``run_pipeline()`` function that orchestrates the full
climate data pipeline without interactive prompts.
"""

from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from pydantic import BaseModel, Field, field_validator, model_validator
from rich.console import Console

from climate_zarr.climate_config import ClimateConfig, get_config
from climate_zarr.county_processor import ModernCountyProcessor
from climate_zarr.stack_nc_to_zarr import (
    generate_hierarchical_zarr_path,
    stack_netcdf_to_zarr_hierarchical,
)
from climate_zarr.transform import merge_climate_dataframes

try:
    from climate_zarr.utils.file_discovery import discover_netcdf_files

    HAS_FILE_DISCOVERY = True
except ImportError:
    HAS_FILE_DISCOVERY = False

console = Console()

DEFAULT_THRESHOLDS: Dict[str, float] = {
    "pr": 25.4,
    "tas": 0.0,
    "tasmax": 35.0,
    "tasmin": 0.0,
}

SUPPORTED_VARIABLES = ("pr", "tas", "tasmax", "tasmin")


class PipelineConfig(BaseModel):
    """Validated configuration for the climate pipeline."""

    nc_dir: Optional[Path] = Field(
        default=None,
        description="NetCDF source directory. None to skip conversion.",
    )
    zarr_dir: Optional[Path] = Field(
        default=None,
        description="Zarr store base directory. Defaults to {output_dir}/zarr.",
    )
    shapefile: Optional[Path] = Field(
        default=None,
        description="County shapefile path. Defaults to regional_counties/{region}_counties.shp.",
    )
    region: str = Field(default="conus", description="Geographic region.")
    variables: tuple = Field(
        default=("pr", "tas", "tasmax", "tasmin"),
        description="Climate variables to process.",
    )
    scenario: str = Field(default="ssp370", description="Climate scenario.")
    thresholds: Optional[Dict[str, float]] = Field(
        default=None,
        description="Per-variable threshold overrides.",
    )
    output_dir: Path = Field(
        default=Path("climate_outputs"),
        description="Base output directory.",
    )
    output_file: Optional[Path] = Field(
        default=None,
        description="Final CSV path. Defaults to {output_dir}/transformed/{region}_{scenario}_climate_stats.csv.",
    )
    n_workers: int = Field(default=4, ge=1, description="Number of workers.")

    @field_validator("region")
    @classmethod
    def validate_region(cls, region_value: str) -> str:
        config = get_config()
        if region_value.lower() not in config.regions:
            available_regions = list(config.regions.keys())
            raise ValueError(
                f"Unknown region '{region_value}'. Available: {available_regions}"
            )
        return region_value.lower()

    @field_validator("variables", mode="before")
    @classmethod
    def validate_variables(cls, variables_value) -> tuple:
        variables_tuple = tuple(variables_value)
        for variable_name in variables_tuple:
            if variable_name not in SUPPORTED_VARIABLES:
                raise ValueError(
                    f"Unsupported variable '{variable_name}'. "
                    f"Supported: {SUPPORTED_VARIABLES}"
                )
        return variables_tuple

    @model_validator(mode="after")
    def set_defaults(self) -> "PipelineConfig":
        if self.zarr_dir is None:
            self.zarr_dir = self.output_dir / "zarr"
        if self.shapefile is None:
            self.shapefile = Path(f"regional_counties/{self.region}_counties.shp")
        if self.output_file is None:
            self.output_file = (
                self.output_dir
                / "transformed"
                / f"{self.region}_{self.scenario}_climate_stats.csv"
            )
        if self.thresholds is None:
            self.thresholds = dict(DEFAULT_THRESHOLDS)
        else:
            merged_thresholds = dict(DEFAULT_THRESHOLDS)
            merged_thresholds.update(self.thresholds)
            self.thresholds = merged_thresholds
        return self


class PipelineResult(BaseModel):
    """Result returned by ``run_pipeline``."""

    model_config = {"arbitrary_types_allowed": True}

    merged_df: pd.DataFrame = Field(description="Final merged output DataFrame.")
    output_path: Optional[Path] = Field(
        default=None, description="Path where CSV was written."
    )
    per_variable: Dict[str, pd.DataFrame] = Field(
        default_factory=dict,
        description="Intermediate per-variable DataFrames.",
    )
    variables_processed: List[str] = Field(default_factory=list)
    variables_skipped: List[str] = Field(default_factory=list)
    zarr_paths: Dict[str, Path] = Field(default_factory=dict)


def run_pipeline(
    nc_dir: Optional[Path] = None,
    shapefile: Optional[Path] = None,
    region: str = "conus",
    variables: Sequence[str] = ("pr", "tas", "tasmax", "tasmin"),
    scenario: str = "ssp370",
    *,
    zarr_dir: Optional[Path] = None,
    thresholds: Optional[Dict[str, float]] = None,
    output_dir: Path = Path("climate_outputs"),
    output_file: Optional[Path] = None,
    n_workers: int = 4,
) -> PipelineResult:
    """Run the full climate data pipeline.

    Parameters
    ----------
    nc_dir : Path, optional
        Directory containing NetCDF files. ``None`` to skip conversion
        and use existing Zarr stores.
    shapefile : Path, optional
        County shapefile path.
    region : str
        Geographic region (validated against ClimateConfig.regions).
    variables : sequence of str
        Climate variables to process.
    scenario : str
        Climate scenario identifier.
    zarr_dir : Path, optional
        Base directory for Zarr stores.
    thresholds : dict, optional
        Per-variable threshold overrides.
    output_dir : Path
        Base output directory.
    output_file : Path, optional
        Explicit CSV output path.
    n_workers : int
        Number of parallel workers.

    Returns
    -------
    PipelineResult
        Contains merged DataFrame, per-variable DataFrames, and metadata.
    """
    # Build and validate configuration upfront.
    pipeline_config = PipelineConfig(
        nc_dir=nc_dir,
        zarr_dir=zarr_dir,
        shapefile=shapefile,
        region=region,
        variables=tuple(variables),
        scenario=scenario,
        thresholds=thresholds,
        output_dir=output_dir,
        output_file=output_file,
        n_workers=n_workers,
    )

    console.print(f"[bold]Pipeline: region={pipeline_config.region}, "
                  f"scenario={pipeline_config.scenario}, "
                  f"variables={pipeline_config.variables}[/bold]")

    zarr_paths: Dict[str, Path] = {}
    per_variable_dataframes: Dict[str, pd.DataFrame] = {}
    variables_processed: List[str] = []
    variables_skipped: List[str] = []

    # ------------------------------------------------------------------
    # Stage 1: NetCDF -> Zarr (optional)
    # ------------------------------------------------------------------
    if pipeline_config.nc_dir is not None:
        console.print("[bold cyan]Stage 1: NetCDF -> Zarr conversion[/bold cyan]")
        for variable_name in pipeline_config.variables:
            console.print(f"[cyan]Discovering NetCDF files for {variable_name}...[/cyan]")

            if HAS_FILE_DISCOVERY:
                nc_files = discover_netcdf_files(
                    directory=pipeline_config.nc_dir / variable_name,
                    pattern="*.nc",
                    validate=True,
                    verbose=False,
                    fail_on_invalid=False,
                )
            else:
                nc_search_directory = pipeline_config.nc_dir / variable_name
                if nc_search_directory.exists():
                    nc_files = sorted(
                        f
                        for f in nc_search_directory.glob("*.nc")
                        if not f.name.startswith("._")
                    )
                else:
                    nc_files = []

            if not nc_files:
                console.print(
                    f"[yellow]No NetCDF files found for {variable_name}, skipping conversion[/yellow]"
                )
                continue

            zarr_output_path = stack_netcdf_to_zarr_hierarchical(
                nc_files=nc_files,
                variable=variable_name,
                region=pipeline_config.region,
                scenario=pipeline_config.scenario,
                base_zarr_dir=pipeline_config.zarr_dir,
            )
            zarr_paths[variable_name] = zarr_output_path
            console.print(f"[green]Converted {variable_name}: {zarr_output_path}[/green]")
    else:
        console.print("[bold cyan]Stage 1: Skipped (no nc_dir provided)[/bold cyan]")

    # Compute expected zarr paths for variables that weren't just converted.
    for variable_name in pipeline_config.variables:
        if variable_name not in zarr_paths:
            zarr_paths[variable_name] = generate_hierarchical_zarr_path(
                base_dir=pipeline_config.zarr_dir,
                variable=variable_name,
                region=pipeline_config.region,
                scenario=pipeline_config.scenario,
            )

    # ------------------------------------------------------------------
    # Stage 2: Zarr -> County Stats
    # ------------------------------------------------------------------
    console.print("[bold cyan]Stage 2: Zarr -> County statistics[/bold cyan]")

    with ModernCountyProcessor(n_workers=pipeline_config.n_workers) as processor:
        county_geodataframe = processor.prepare_shapefile(pipeline_config.shapefile)
        console.print(
            f"[green]Loaded shapefile: {len(county_geodataframe)} counties[/green]"
        )

        for variable_name in pipeline_config.variables:
            variable_zarr_path = zarr_paths[variable_name]

            if not variable_zarr_path.exists():
                console.print(
                    f"[yellow]Zarr not found for {variable_name}: "
                    f"{variable_zarr_path}, skipping[/yellow]"
                )
                variables_skipped.append(variable_name)
                continue

            threshold_value = pipeline_config.thresholds.get(
                variable_name, DEFAULT_THRESHOLDS.get(variable_name, 0.0)
            )
            console.print(
                f"[cyan]Processing {variable_name} "
                f"(threshold={threshold_value})...[/cyan]"
            )

            variable_dataframe = processor.process_zarr_data(
                zarr_path=variable_zarr_path,
                gdf=county_geodataframe,
                scenario=pipeline_config.scenario,
                variable=variable_name,
                threshold=threshold_value,
            )
            per_variable_dataframes[variable_name] = variable_dataframe
            variables_processed.append(variable_name)
            console.print(
                f"[green]{variable_name}: {len(variable_dataframe)} rows[/green]"
            )

    # ------------------------------------------------------------------
    # Stage 3: Merge & Transform
    # ------------------------------------------------------------------
    console.print("[bold cyan]Stage 3: Merge & transform[/bold cyan]")

    if not per_variable_dataframes:
        console.print("[red]No variables produced results. Returning empty result.[/red]")
        return PipelineResult(
            merged_df=pd.DataFrame(),
            per_variable=per_variable_dataframes,
            variables_processed=variables_processed,
            variables_skipped=variables_skipped,
            zarr_paths=zarr_paths,
        )

    merged_dataframe = merge_climate_dataframes(per_variable_dataframes)

    # Save to CSV.
    output_csv_path = pipeline_config.output_file
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    merged_dataframe.to_csv(output_csv_path, index=False)
    console.print(f"[green]Saved: {output_csv_path} ({len(merged_dataframe)} rows)[/green]")

    return PipelineResult(
        merged_df=merged_dataframe,
        output_path=output_csv_path,
        per_variable=per_variable_dataframes,
        variables_processed=variables_processed,
        variables_skipped=variables_skipped,
        zarr_paths=zarr_paths,
    )
