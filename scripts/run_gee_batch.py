#!/usr/bin/env python
"""Run the GEE batch export pipeline for a full scenario.

Usage:
    uv run python scripts/run_gee_batch.py
"""

from pathlib import Path

from climate_zarr.gee import GEEConfig, GEEPipelineConfig, run_gee_pipeline

config = GEEPipelineConfig(
    gee=GEEConfig(
        project_id="ee-chrismihiar",
    ),
    models=["NorESM2-LM"],
    scenarios=["ssp585"],
    variables=("pr", "tas", "tasmax", "tasmin"),
    year_range=(2015, 2100),
    region="conus",
    output_dir=Path("climate_outputs"),
    use_batch_export=True,
)

if __name__ == "__main__":
    result = run_gee_pipeline(config)
    print(f"\nDone — {len(result.merged_df)} rows written to {result.output_path}")
    print(f"Variables processed: {result.variables_processed}")
    print(f"Variables skipped:   {result.variables_skipped}")
