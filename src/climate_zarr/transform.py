"""Merge and transform climate variable DataFrames into final output format.

Operates on in-memory DataFrames produced by ModernCountyProcessor,
merging per-variable results into the standardized output matching
climate_output_format.yaml.
"""

from typing import Dict

import numpy as np
import pandas as pd
from rich.console import Console

console = Console()

# Column mappings from processor output columns to target output format.
# Keys are the source column names produced by data_utils, values are the
# target column names matching climate_output_format.yaml.
VARIABLE_COLUMN_MAPPINGS: Dict[str, Dict[str, str]] = {
    "pr": {
        "days_above_threshold": "daysabove1in",
        "total_annual_precip_mm": "annual_total_precip",
    },
    "tas": {
        "mean_annual_temp_c": "annual_mean_temp",
    },
    "tasmax": {
        "mean_annual_tasmax_c": "tmaxavg",
        "heat_index_days": "daysabove90F",
    },
    "tasmin": {},
}

TARGET_COLUMNS = [
    "cid2",
    "year",
    "scenario",
    "name",
    "daysabove1in",
    "daysabove90F",
    "tmaxavg",
    "annual_mean_temp",
    "annual_total_precip",
]


def merge_climate_dataframes(
    per_variable: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Merge per-variable DataFrames into a single output matching target format.

    Parameters
    ----------
    per_variable : dict[str, DataFrame]
        Mapping of variable name (pr, tas, tasmax, tasmin) to the DataFrame
        produced by ``ModernCountyProcessor.process_zarr_data``.

    Returns
    -------
    DataFrame
        Merged and transformed DataFrame with columns matching TARGET_COLUMNS.
    """
    if not per_variable:
        raise ValueError("No variable DataFrames provided for merging")

    merge_key_cols = ["county_id", "year", "scenario"]

    # Determine which variable has the most rows — start merging from it.
    primary_variable = max(per_variable, key=lambda v: len(per_variable[v]))
    console.print(
        f"[blue]Using {primary_variable} as primary merge variable "
        f"({len(per_variable[primary_variable])} rows)[/blue]"
    )

    merged_dataframe = per_variable[primary_variable].copy()

    # Merge remaining variables via left join on the merge key.
    for variable_name, variable_dataframe in per_variable.items():
        if variable_name == primary_variable:
            continue

        # Only bring in columns not already present (except merge keys).
        existing_columns = set(merged_dataframe.columns)
        new_columns = [
            col
            for col in variable_dataframe.columns
            if col not in existing_columns or col in merge_key_cols
        ]

        merged_dataframe = merged_dataframe.merge(
            variable_dataframe[new_columns],
            on=merge_key_cols,
            how="left",
            suffixes=("", f"_{variable_name}"),
        )
        console.print(f"[blue]Merged {variable_name} ({len(variable_dataframe)} rows)[/blue]")

    # Apply column renames per variable mapping.
    for variable_name, column_mapping in VARIABLE_COLUMN_MAPPINGS.items():
        for source_column, target_column in column_mapping.items():
            if source_column in merged_dataframe.columns:
                merged_dataframe[target_column] = merged_dataframe[source_column]

    # Create cid2 (integer FIPS code) from county_id.
    merged_dataframe["cid2"] = pd.to_numeric(
        merged_dataframe["county_id"], errors="coerce"
    ).astype("Int64")

    # Create formatted name as "county_name, state".
    if "county_name" in merged_dataframe.columns and "state" in merged_dataframe.columns:
        county_name_column = merged_dataframe["county_name"].fillna("")
        state_column = merged_dataframe["state"].fillna("")
        merged_dataframe["name"] = county_name_column + ", " + state_column
        merged_dataframe["name"] = merged_dataframe["name"].str.replace(
            r", $", "", regex=True
        )
    else:
        merged_dataframe["name"] = merged_dataframe["county_id"].astype(str)

    # Ensure all target columns exist, filling missing ones with NaN.
    for target_column in TARGET_COLUMNS:
        if target_column not in merged_dataframe.columns:
            merged_dataframe[target_column] = np.nan

    # Select and order to target format.
    result_dataframe = merged_dataframe[TARGET_COLUMNS].copy()

    # Apply type coercions.
    integer_columns = ["cid2", "daysabove1in", "daysabove90F"]
    numeric_columns = ["tmaxavg", "annual_mean_temp", "annual_total_precip"]

    for column_name in integer_columns:
        result_dataframe[column_name] = pd.to_numeric(
            result_dataframe[column_name], errors="coerce"
        ).astype("Int64")

    for column_name in numeric_columns:
        result_dataframe[column_name] = pd.to_numeric(
            result_dataframe[column_name], errors="coerce"
        )

    result_dataframe["year"] = pd.to_numeric(
        result_dataframe["year"], errors="coerce"
    ).astype("Int64")

    console.print(f"[green]Merged output: {len(result_dataframe)} rows, {len(TARGET_COLUMNS)} columns[/green]")
    return result_dataframe
