#!/usr/bin/env python
"""Run the GEE batch export pipeline for NorESM2-LM historical (1950-2014)."""

from pathlib import Path

from climate_zarr.gee import GEEConfig, GEEPipelineConfig, run_gee_pipeline

config = GEEPipelineConfig(
    gee=GEEConfig(
        project_id="ee-chrismihiar",
    ),
    models=["NorESM2-LM"],
    scenarios=["historical"],
    variables=("pr", "tas", "tasmax", "tasmin"),
    year_range=(1950, 2014),
    region="conus",
    output_dir=Path("climate_outputs"),
    use_batch_export=True,
)

if __name__ == "__main__":
    result = run_gee_pipeline(config)
    print(f"\nDone — {len(result.merged_df)} rows written to {result.output_path}")
    print(f"Variables processed: {result.variables_processed}")
    print(f"Variables skipped:   {result.variables_skipped}")
