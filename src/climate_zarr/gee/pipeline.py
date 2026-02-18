"""Top-level orchestration for the Google Earth Engine pipeline.

Produces the same ``PipelineResult`` and final CSV as the local
NetCDF -> Zarr -> county pipeline, but sources data entirely from GEE.
"""

from typing import Dict, List

import pandas as pd
from rich.console import Console

from climate_zarr.gee.client import initialize_gee, get_county_features
from climate_zarr.gee.config import GEEPipelineConfig
from climate_zarr.gee.extract import build_variable_dataframe
from climate_zarr.pipeline import PipelineResult
from climate_zarr.transform import merge_climate_dataframes

console = Console()


def run_gee_pipeline(config: GEEPipelineConfig) -> PipelineResult:
    """Run the full GEE-based climate pipeline.

    1. Initialize GEE authentication.
    2. Load county features from the TIGER asset.
    3. For each model x scenario x variable, build per-variable DataFrames
       via server-side GEE reduction + ``getInfo()`` extraction.
    4. Merge per-variable DataFrames using the shared ``merge_climate_dataframes``.
    5. Write final CSV and return ``PipelineResult``.

    Parameters
    ----------
    config : GEEPipelineConfig
        Validated pipeline configuration.

    Returns
    -------
    PipelineResult
        Same result type as ``climate_zarr.pipeline.run_pipeline``.
    """
    gee_config = config.gee

    console.print("[bold]GEE Pipeline[/bold]")
    console.print(
        f"  models:    {config.models}\n"
        f"  scenarios: {config.scenarios}\n"
        f"  variables: {config.variables}\n"
        f"  years:     {config.year_range[0]}-{config.year_range[1]}\n"
        f"  region:    {config.region}"
    )

    # Stage 1: Initialize GEE
    console.print("[bold cyan]Stage 1: Initialize GEE[/bold cyan]")
    initialize_gee(gee_config.project_id)

    # Stage 2: Load county features
    console.print("[bold cyan]Stage 2: Load county features[/bold cyan]")
    counties = get_county_features(
        region=config.region,
        county_asset=gee_config.county_asset,
    )

    # Stage 3: Extract per-variable DataFrames
    console.print("[bold cyan]Stage 3: Extract climate variables from GEE[/bold cyan]")

    per_variable_dataframes: Dict[str, pd.DataFrame] = {}
    variables_processed: List[str] = []
    variables_skipped: List[str] = []

    for model_name in config.models:
        for scenario_name in config.scenarios:
            console.print(
                f"[bold]Processing model={model_name}, scenario={scenario_name}[/bold]"
            )

            for variable_name in config.variables:
                console.print(f"[cyan]Extracting {variable_name}...[/cyan]")
                try:
                    variable_dataframe = build_variable_dataframe(
                        variable=variable_name,
                        year_range=config.year_range,
                        model=model_name,
                        scenario=scenario_name,
                        counties=counties,
                        collection_id=gee_config.collection_id,
                        scale=gee_config.scale,
                        batch_size=gee_config.batch_size,
                    )

                    if variable_dataframe.empty:
                        console.print(
                            f"[yellow]{variable_name}: no data returned, skipping[/yellow]"
                        )
                        variables_skipped.append(variable_name)
                        continue

                    # If processing multiple models, tag the rows
                    if len(config.models) > 1:
                        variable_dataframe["model"] = model_name

                    # Accumulate: if a variable key already exists (from a
                    # prior model), concatenate the DataFrames.
                    if variable_name in per_variable_dataframes:
                        per_variable_dataframes[variable_name] = pd.concat(
                            [per_variable_dataframes[variable_name], variable_dataframe],
                            ignore_index=True,
                        )
                    else:
                        per_variable_dataframes[variable_name] = variable_dataframe

                    variables_processed.append(variable_name)
                    console.print(
                        f"[green]{variable_name}: {len(variable_dataframe)} rows[/green]"
                    )

                except Exception as error:
                    console.print(
                        f"[red]{variable_name} failed: {error}[/red]"
                    )
                    variables_skipped.append(variable_name)

    # Stage 4: Merge & Transform
    console.print("[bold cyan]Stage 4: Merge & transform[/bold cyan]")

    if not per_variable_dataframes:
        console.print("[red]No variables produced results. Returning empty result.[/red]")
        return PipelineResult(
            merged_df=pd.DataFrame(),
            per_variable=per_variable_dataframes,
            variables_processed=variables_processed,
            variables_skipped=variables_skipped,
        )

    merged_dataframe = merge_climate_dataframes(per_variable_dataframes)

    # Save to CSV
    output_csv_path = config.output_file
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    merged_dataframe.to_csv(output_csv_path, index=False)
    console.print(
        f"[green]Saved: {output_csv_path} ({len(merged_dataframe)} rows)[/green]"
    )

    return PipelineResult(
        merged_df=merged_dataframe,
        output_path=output_csv_path,
        per_variable=per_variable_dataframes,
        variables_processed=variables_processed,
        variables_skipped=variables_skipped,
    )
