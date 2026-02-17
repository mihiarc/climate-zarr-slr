"""
Climate Zarr SLR - Sea Level Rise Research Pipeline

Specialized climate data pipeline for studying the impact of climate change on
population and income in a sea level rise (SLR) context. Built on the Climate
Zarr Toolkit, this application provides tools for analyzing climate's socioeconomic
impacts through NetCDF to Zarr conversion and county-level statistical analysis.

Research Focus:
- Climate impacts on coastal populations and demographics
- Economic vulnerability to climate change and SLR
- County-level climate statistics for socioeconomic modeling
- Regional climate patterns affecting at-risk communities
"""

from climate_zarr._version import __version__

__author__ = "Chris Mihiar"
__email__ = "chris.mihiar.fs@gmail.com"

# Public API exports
from climate_zarr.climate_config import (
    ClimateConfig,
    CompressionConfig,
    ChunkingConfig,
    RegionConfig,
    ProcessingConfig,
    get_config,
)
from climate_zarr.stack_nc_to_zarr import (
    stack_netcdf_to_zarr,
    stack_netcdf_to_zarr_hierarchical,
    generate_hierarchical_zarr_path,
)
from climate_zarr.county_processor import ModernCountyProcessor
from climate_zarr.pipeline import PipelineConfig, PipelineResult, run_pipeline
from climate_zarr.transform import merge_climate_dataframes

__all__ = [
    # Version info
    "__version__",
    "__author__",
    "__email__",
    # Configuration
    "ClimateConfig",
    "CompressionConfig",
    "ChunkingConfig",
    "RegionConfig",
    "ProcessingConfig",
    "get_config",
    # Core functions
    "stack_netcdf_to_zarr",
    "stack_netcdf_to_zarr_hierarchical",
    "generate_hierarchical_zarr_path",
    "ModernCountyProcessor",
    # Pipeline API
    "run_pipeline",
    "PipelineConfig",
    "PipelineResult",
    "merge_climate_dataframes",
]
