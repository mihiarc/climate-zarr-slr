"""Top-level orchestration for the Google Earth Engine pipeline.

Produces the same ``PipelineResult`` and final CSV as the local
NetCDF -> Zarr -> county pipeline, but sources data entirely from GEE.

Supports two execution modes:
- **Sequential** (default) — iterates (variable, year) batches via
  ``getInfo()`` calls.
- **Batch export** (``use_batch_export=True``) — submits all tasks to
  GEE's batch export queue, polls for completion, then reads results
  back.  ~15-20x faster for large runs.
"""

from typing import Dict, List

import pandas as pd
from rich.console import Console

from climate_zarr.gee.client import initialize_gee, get_county_features
from climate_zarr.gee.config import GEEPipelineConfig
from climate_zarr.gee.extract import (
    build_feature_collection,
    build_variable_dataframe,
    postprocess_variable_dataframe,
)
from climate_zarr.gee.tasks import (
    TaskSpec,
    cleanup_exports,
    poll_tasks,
    read_task_result,
    submit_export_tasks,
)
from climate_zarr.pipeline import PipelineResult
from climate_zarr.transform import merge_climate_dataframes

console = Console()


def run_gee_pipeline(config: GEEPipelineConfig) -> PipelineResult:
    """Run the full GEE-based climate pipeline.

    Dispatches to sequential or batch export mode based on
    ``config.use_batch_export``.

    Parameters
    ----------
    config : GEEPipelineConfig
        Validated pipeline configuration.

    Returns
    -------
    PipelineResult
        Same result type as ``climate_zarr.pipeline.run_pipeline``.
    """
    if config.use_batch_export:
        return run_gee_pipeline_batch(config)
    return _run_gee_pipeline_sequential(config)


def _run_gee_pipeline_sequential(config: GEEPipelineConfig) -> PipelineResult:
    """Run the sequential (getInfo-based) GEE pipeline.

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

    console.print("[bold]GEE Pipeline (sequential)[/bold]")
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


def run_gee_pipeline_batch(config: GEEPipelineConfig) -> PipelineResult:
    """Run the batch-export GEE pipeline.

    Submits all (variable, year) combos as GEE export tasks, polls
    until completion, reads results back, and assembles the same
    output as the sequential pipeline.

    Stages:
    1. Initialize GEE
    2. Load county features
    3. Build FeatureCollections + submit export tasks
    4. Poll until all tasks reach terminal state
    5. Read results and post-process per variable
    6. Cleanup temporary exports
    7. Merge & write CSV

    Parameters
    ----------
    config : GEEPipelineConfig
        Validated pipeline configuration with ``use_batch_export=True``.

    Returns
    -------
    PipelineResult
        Same result type as the sequential pipeline.
    """
    gee_config = config.gee
    start_year, end_year = config.year_range
    all_years = list(range(start_year, end_year + 1))
    total_tasks = (
        len(config.models)
        * len(config.scenarios)
        * len(config.variables)
        * len(all_years)
    )

    console.print("[bold]GEE Pipeline (batch export)[/bold]")
    console.print(
        f"  models:    {config.models}\n"
        f"  scenarios: {config.scenarios}\n"
        f"  variables: {config.variables}\n"
        f"  years:     {start_year}-{end_year} ({len(all_years)} years)\n"
        f"  region:    {config.region}\n"
        f"  backend:   {gee_config.export_backend.value}\n"
        f"  tasks:     {total_tasks}"
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

    # Stage 3: Build all FeatureCollections + submit export tasks
    console.print(
        "[bold cyan]Stage 3: Build FeatureCollections & submit tasks[/bold cyan]"
    )

    all_specs: list[TaskSpec] = []

    for model_name in config.models:
        for scenario_name in config.scenarios:
            for variable_name in config.variables:
                for year in all_years:
                    feature_collection = build_feature_collection(
                        variable=variable_name,
                        year=year,
                        model=model_name,
                        scenario=scenario_name,
                        counties=counties,
                        collection_id=gee_config.collection_id,
                        scale=gee_config.scale,
                    )
                    spec = TaskSpec(
                        variable=variable_name,
                        year=year,
                        model=model_name,
                        scenario=scenario_name,
                        feature_collection=feature_collection,
                    )
                    all_specs.append(spec)

    console.print(f"  [blue]Built {len(all_specs)} FeatureCollections[/blue]")

    submit_export_tasks(all_specs, gee_config)

    # Stage 4: Poll until all tasks complete
    console.print("[bold cyan]Stage 4: Poll task status[/bold cyan]")
    poll_tasks(all_specs, poll_interval=gee_config.poll_interval_seconds)

    # Stage 5: Read results and post-process per variable
    console.print("[bold cyan]Stage 5: Read results & post-process[/bold cyan]")

    per_variable_dataframes: Dict[str, pd.DataFrame] = {}
    variables_processed: List[str] = []
    variables_skipped: List[str] = []

    # Group specs by (model, scenario, variable) for accumulation
    variable_raw_frames: Dict[str, list[pd.DataFrame]] = {}

    for spec in all_specs:
        if spec.state != "COMPLETED":
            console.print(
                f"  [yellow]Skipping {spec.description}: "
                f"state={spec.state}[/yellow]"
            )
            continue

        try:
            raw_dataframe = read_task_result(spec, gee_config)
            if raw_dataframe.empty:
                continue

            variable_key = spec.variable
            if variable_key not in variable_raw_frames:
                variable_raw_frames[variable_key] = []
            variable_raw_frames[variable_key].append(raw_dataframe)

        except Exception as error:
            console.print(
                f"  [red]Failed to read {spec.description}: {error}[/red]"
            )

    for variable_name, raw_frames in variable_raw_frames.items():
        if not raw_frames:
            variables_skipped.append(variable_name)
            continue

        combined_raw = pd.concat(raw_frames, ignore_index=True)
        processed_dataframe = postprocess_variable_dataframe(
            combined_raw, variable_name
        )

        if processed_dataframe.empty:
            variables_skipped.append(variable_name)
            continue

        # Tag model column if processing multiple models
        if len(config.models) > 1:
            # Model info isn't in the export by default — reconstruct from specs
            model_year_map = {
                (s.year, s.scenario): s.model
                for s in all_specs
                if s.variable == variable_name and s.state == "COMPLETED"
            }
            processed_dataframe["model"] = processed_dataframe.apply(
                lambda row: model_year_map.get(
                    (row["year"], row["scenario"]), config.models[0]
                ),
                axis=1,
            )

        per_variable_dataframes[variable_name] = processed_dataframe
        variables_processed.append(variable_name)
        console.print(
            f"  [green]{variable_name}: {len(processed_dataframe)} rows "
            f"({processed_dataframe['county_id'].nunique()} counties, "
            f"{processed_dataframe['year'].nunique()} years)[/green]"
        )

    # Stage 6: Cleanup temporary exports
    if not config.skip_cleanup:
        console.print("[bold cyan]Stage 6: Cleanup[/bold cyan]")
        cleanup_exports(all_specs, gee_config)
    else:
        console.print(
            "[bold cyan]Stage 6: Cleanup skipped (skip_cleanup=True)[/bold cyan]"
        )

    # Log failed tasks
    failed_specs = [s for s in all_specs if s.state == "FAILED"]
    if failed_specs:
        console.print(
            f"[yellow]{len(failed_specs)} tasks failed — "
            f"partial results used[/yellow]"
        )
        for spec in failed_specs:
            console.print(
                f"  [dim]{spec.description}: {spec.error_message}[/dim]"
            )

    # Stage 7: Merge & write CSV
    console.print("[bold cyan]Stage 7: Merge & write CSV[/bold cyan]")

    if not per_variable_dataframes:
        console.print("[red]No variables produced results. Returning empty result.[/red]")
        return PipelineResult(
            merged_df=pd.DataFrame(),
            per_variable=per_variable_dataframes,
            variables_processed=variables_processed,
            variables_skipped=variables_skipped,
        )

    merged_dataframe = merge_climate_dataframes(per_variable_dataframes)

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
