#!/usr/bin/env python
"""Run the GEE batch export pipeline for ACCESS-CM2 (historical + ssp245 + ssp585)."""

from pathlib import Path

from climate_zarr.gee import GEEConfig, GEEPipelineConfig, run_gee_pipeline

# Historical: 1950-2014
historical_config = GEEPipelineConfig(
    gee=GEEConfig(
        project_id="ee-chrismihiar",
    ),
    models=["ACCESS-CM2"],
    scenarios=["historical"],
    variables=("pr", "tas", "tasmax", "tasmin"),
    year_range=(1950, 2014),
    region="conus",
    output_dir=Path("climate_outputs"),
    use_batch_export=True,
)

# SSP245: 2015-2100
ssp245_config = GEEPipelineConfig(
    gee=GEEConfig(
        project_id="ee-chrismihiar",
    ),
    models=["ACCESS-CM2"],
    scenarios=["ssp245"],
    variables=("pr", "tas", "tasmax", "tasmin"),
    year_range=(2015, 2100),
    region="conus",
    output_dir=Path("climate_outputs"),
    use_batch_export=True,
)

# SSP585: 2015-2100
ssp585_config = GEEPipelineConfig(
    gee=GEEConfig(
        project_id="ee-chrismihiar",
    ),
    models=["ACCESS-CM2"],
    scenarios=["ssp585"],
    variables=("pr", "tas", "tasmax", "tasmin"),
    year_range=(2015, 2100),
    region="conus",
    output_dir=Path("climate_outputs"),
    use_batch_export=True,
)

if __name__ == "__main__":
    for label, cfg in [
        ("ACCESS-CM2 historical", historical_config),
        ("ACCESS-CM2 ssp245", ssp245_config),
        ("ACCESS-CM2 ssp585", ssp585_config),
    ]:
        print(f"\n{'='*60}")
        print(f"  {label}")
        print(f"{'='*60}")
        result = run_gee_pipeline(cfg)
        print(f"Done — {len(result.merged_df)} rows -> {result.output_path}")
