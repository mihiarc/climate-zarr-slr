"""Pull GEE FeatureCollection results into pandas DataFrames.

Handles batching by year to respect GEE ``getInfo()`` timeout limits,
and assembles per-variable DataFrames whose column schemas match the
existing local pipeline output so ``transform.merge_climate_dataframes``
works unmodified.
"""

import time
from typing import Callable

import ee
import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from climate_zarr.gee.reducers import VARIABLE_REDUCERS

console = Console()

# Columns produced by each variable's reducer that we need in the DataFrame.
# These must match the column names that transform.py expects.
VARIABLE_OUTPUT_COLUMNS: dict[str, list[str]] = {
    "pr": [
        "county_id", "county_name", "state", "year", "scenario",
        "total_annual_precip_mm", "days_above_threshold",
    ],
    "tas": [
        "county_id", "county_name", "state", "year", "scenario",
        "mean_annual_temp_c",
    ],
    "tasmax": [
        "county_id", "county_name", "state", "year", "scenario",
        "mean_annual_tasmax_c", "heat_index_days",
    ],
    "tasmin": [
        "county_id", "county_name", "state", "year", "scenario",
        "cold_days",
    ],
}


def extract_to_dataframe(
    feature_collection: ee.FeatureCollection,
    max_retries: int = 3,
    base_delay: float = 30.0,
) -> pd.DataFrame:
    """Convert a GEE FeatureCollection to a pandas DataFrame.

    Calls ``getInfo()`` which transfers data from GEE servers.  The
    caller should keep the FeatureCollection small enough to avoid
    timeouts (~5 min limit for interactive calls).

    Retries with exponential backoff on transient connection errors
    (e.g. laptop sleep causing ``RemoteDisconnected``).

    Parameters
    ----------
    feature_collection : ee.FeatureCollection
        Server-side collection of features with properties.
    max_retries : int
        Number of retry attempts on transient failures.
    base_delay : float
        Initial delay in seconds before first retry (doubles each attempt).

    Returns
    -------
    pd.DataFrame
        One row per feature, columns from feature properties.

    Raises
    ------
    RuntimeError
        If the GEE ``getInfo()`` call fails after all retries.
    """
    last_error = None
    for attempt in range(1 + max_retries):
        try:
            info = feature_collection.getInfo()
            break
        except Exception as error:
            last_error = error
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                console.print(
                    f"  [yellow]getInfo() failed (attempt {attempt + 1}/{1 + max_retries}): "
                    f"{error} — retrying in {delay:.0f}s[/yellow]"
                )
                time.sleep(delay)
            else:
                raise RuntimeError(
                    f"GEE getInfo() failed after {1 + max_retries} attempts: {last_error}"
                ) from last_error

    features = info.get("features", [])
    if not features:
        return pd.DataFrame()

    rows = [feature["properties"] for feature in features]
    return pd.DataFrame(rows)


def process_variable_year_batch(
    variable: str,
    years: list[int],
    model: str,
    scenario: str,
    counties: ee.FeatureCollection,
    collection_id: str = "NASA/GDDP-CMIP6",
    scale: int = 27830,
) -> pd.DataFrame:
    """Process a batch of years for one variable and return a DataFrame.

    Merges server-side FeatureCollections for each year in the batch,
    then pulls results with a single ``getInfo()`` call.

    Parameters
    ----------
    variable : str
        Climate variable name.
    years : list[int]
        Years to process in this batch.
    model, scenario : str
        CMIP6 model and SSP scenario.
    counties : ee.FeatureCollection
        County features (already filtered by region).
    collection_id : str
        GEE ImageCollection asset.
    scale : int
        Processing resolution in meters.

    Returns
    -------
    pd.DataFrame
        Rows for every county x year in the batch.
    """
    reducer_function: Callable = VARIABLE_REDUCERS[variable]

    # Build a merged FeatureCollection for all years in the batch
    year_collections = []
    for year in years:
        year_feature_collection = reducer_function(
            year=year,
            model=model,
            scenario=scenario,
            counties=counties,
            collection_id=collection_id,
            scale=scale,
        )
        year_collections.append(year_feature_collection)

    # Merge all year collections into one for a single getInfo call
    merged_collection = ee.FeatureCollection(year_collections).flatten()

    batch_dataframe = extract_to_dataframe(merged_collection)
    return batch_dataframe


def build_variable_dataframe(
    variable: str,
    year_range: tuple[int, int],
    model: str,
    scenario: str,
    counties: ee.FeatureCollection,
    collection_id: str = "NASA/GDDP-CMIP6",
    scale: int = 27830,
    batch_size: int = 5,
) -> pd.DataFrame:
    """Build the complete per-variable DataFrame over a year range.

    Iterates year batches with a rich progress bar.  The returned
    DataFrame has the same column schema as the existing local
    pipeline's per-variable output.

    Parameters
    ----------
    variable : str
        Climate variable name.
    year_range : tuple[int, int]
        Inclusive (start_year, end_year).
    model, scenario : str
        CMIP6 model and SSP scenario.
    counties : ee.FeatureCollection
        County features.
    collection_id : str
        GEE asset path.
    scale : int
        Processing resolution in meters.
    batch_size : int
        Years per ``getInfo()`` call.

    Returns
    -------
    pd.DataFrame
        Complete per-variable DataFrame with columns matching
        ``VARIABLE_OUTPUT_COLUMNS[variable]``.
    """
    start_year, end_year = year_range
    all_years = list(range(start_year, end_year + 1))
    batches = [
        all_years[i : i + batch_size]
        for i in range(0, len(all_years), batch_size)
    ]

    console.print(
        f"[blue]Building {variable} DataFrame: {len(all_years)} years in "
        f"{len(batches)} batches (batch_size={batch_size})[/blue]"
    )

    batch_dataframes: list[pd.DataFrame] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console,
    ) as progress:
        task = progress.add_task(
            f"GEE {variable} ({model}/{scenario})", total=len(batches)
        )

        for batch_years in batches:
            try:
                batch_dataframe = process_variable_year_batch(
                    variable=variable,
                    years=batch_years,
                    model=model,
                    scenario=scenario,
                    counties=counties,
                    collection_id=collection_id,
                    scale=scale,
                )
                if not batch_dataframe.empty:
                    batch_dataframes.append(batch_dataframe)
                    console.print(
                        f"  [cyan]Batch {batch_years[0]}-{batch_years[-1]}: "
                        f"{len(batch_dataframe)} rows[/cyan]"
                    )
            except Exception as error:
                console.print(
                    f"  [red]Batch {batch_years[0]}-{batch_years[-1]} failed: "
                    f"{error}[/red]"
                )
            progress.advance(task)

    if not batch_dataframes:
        console.print(f"[yellow]No data returned for {variable}[/yellow]")
        return pd.DataFrame(columns=VARIABLE_OUTPUT_COLUMNS[variable])

    combined_dataframe = pd.concat(batch_dataframes, ignore_index=True)

    # Drop duplicate rows that can occur when GEE's filterBounds returns
    # the same feature multiple times (spatial-index tile boundary artifact).
    dedup_keys = ["county_id", "year", "scenario"]
    before_dedup = len(combined_dataframe)
    combined_dataframe = combined_dataframe.drop_duplicates(subset=dedup_keys)
    dropped = before_dedup - len(combined_dataframe)
    if dropped > 0:
        console.print(
            f"  [yellow]Dropped {dropped} duplicate rows for {variable}[/yellow]"
        )

    # Rename the 'mean' column that reduceRegions produces for single-band
    # reducers. For multi-band images the band names are preserved.
    if "mean" in combined_dataframe.columns:
        # Single-band reducer output -- rename based on variable
        single_band_renames = {
            "tas": "mean_annual_temp_c",
            "tasmin": "cold_days",
        }
        if variable in single_band_renames:
            combined_dataframe = combined_dataframe.rename(
                columns={"mean": single_band_renames[variable]}
            )

    # Ensure expected columns exist, filling missing ones with NaN
    expected_columns = VARIABLE_OUTPUT_COLUMNS[variable]
    for column_name in expected_columns:
        if column_name not in combined_dataframe.columns:
            combined_dataframe[column_name] = None

    # Select only the expected columns (drop GEE-internal properties)
    combined_dataframe = combined_dataframe[
        [col for col in expected_columns if col in combined_dataframe.columns]
    ].copy()

    # Coerce types
    combined_dataframe["year"] = pd.to_numeric(
        combined_dataframe["year"], errors="coerce"
    ).astype("Int64")

    # GEE reduceRegions(mean) returns spatial averages of per-pixel day counts,
    # so count-based columns come back as floats (e.g. 7.68).  Round them to
    # integers so transform.py can safely cast to Int64.
    integer_columns = {"days_above_threshold", "heat_index_days", "cold_days"}
    for column_name in integer_columns:
        if column_name in combined_dataframe.columns:
            combined_dataframe[column_name] = (
                pd.to_numeric(combined_dataframe[column_name], errors="coerce")
                .round(0)
                .astype("Int64")
            )

    # Drop duplicate rows (safety net for a GEE bug where combining
    # filterBounds with property filters can duplicate features).
    before_dedup = len(combined_dataframe)
    dedup_keys = ["county_id", "year", "scenario"]
    combined_dataframe = combined_dataframe.drop_duplicates(
        subset=dedup_keys, keep="first"
    ).reset_index(drop=True)
    dropped = before_dedup - len(combined_dataframe)
    if dropped:
        console.print(f"  [yellow]Dropped {dropped} duplicate rows[/yellow]")

    console.print(
        f"[green]{variable}: {len(combined_dataframe)} total rows "
        f"({combined_dataframe['county_id'].nunique()} counties, "
        f"{combined_dataframe['year'].nunique()} years)[/green]"
    )
    return combined_dataframe
