"""Google Earth Engine data source for the climate pipeline.

This subpackage provides an alternative to the local NetCDF -> Zarr pipeline
by sourcing data from the ``NASA/GDDP-CMIP6`` collection on Google Earth
Engine.  It produces the same per-variable DataFrames and final merged CSV
as the local pipeline.

Usage::

    from climate_zarr.gee import run_gee_pipeline, GEEPipelineConfig, GEEConfig

    config = GEEPipelineConfig(
        gee=GEEConfig(project_id="my-gcp-project"),
        scenarios=["ssp370"],
        year_range=(2020, 2025),
    )
    result = run_gee_pipeline(config)
"""

from climate_zarr.gee.config import GEEConfig, GEEPipelineConfig
from climate_zarr.gee.pipeline import run_gee_pipeline

__all__ = [
    "GEEConfig",
    "GEEPipelineConfig",
    "run_gee_pipeline",
]
