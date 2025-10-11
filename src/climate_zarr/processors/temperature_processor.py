#!/usr/bin/env python
"""Temperature data processor for county-level statistics."""

from pathlib import Path
import pandas as pd
import geopandas as gpd
import xarray as xr
from rich.console import Console

from .base_processor import BaseCountyProcessor
from .region_strategy import get_strategy_for_region, infer_region_from_gdf
from ..utils.data_utils import convert_units

console = Console()


class TemperatureProcessor(BaseCountyProcessor):
    """Processor for mean temperature data (tas variable)."""

    def process_variable_data(
        self, data: xr.DataArray, gdf: gpd.GeoDataFrame, scenario: str, **kwargs
    ) -> pd.DataFrame:
        """Process temperature data for all counties.

        Args:
            data: Temperature data array
            gdf: County geometries
            scenario: Scenario name
            **kwargs: Additional parameters

        Returns:
            DataFrame with temperature statistics
        """
        console.print("[blue]Processing mean temperature data...[/blue]")

        # Convert units from Kelvin to Celsius
        tas_data = convert_units(data, "K", "C")

        # Standardize coordinates
        tas_data = self._standardize_coordinates(tas_data)

        # Select strategy based on region (simple logic: CONUS = chunked, others = vectorized)
        region = kwargs.get("region", infer_region_from_gdf(gdf))
        strategy = get_strategy_for_region(region, gdf, self.n_workers)

        # Process the data (no threshold needed for temperature)
        return strategy.process(
            data=tas_data,
            gdf=gdf,
            variable="tas",
            scenario=scenario,
            threshold=0.0,  # Not used for temperature
            n_workers=self.n_workers,
        )

    def process_zarr_file(
        self, zarr_path: Path, gdf: gpd.GeoDataFrame, scenario: str = "historical"
    ) -> pd.DataFrame:
        """Process a Zarr file containing temperature data.

        Args:
            zarr_path: Path to Zarr dataset
            gdf: County geometries
            scenario: Scenario name

        Returns:
            DataFrame with temperature statistics
        """
        console.print(f"[blue]Opening temperature Zarr dataset:[/blue] {zarr_path}")

        # Open with native Zarr chunks to avoid rechunking overhead
        ds = xr.open_zarr(zarr_path)

        # Get temperature data
        if "tas" not in ds.data_vars:
            raise ValueError("Temperature variable 'tas' not found in dataset")

        tas_data = ds["tas"]

        return self.process_variable_data(data=tas_data, gdf=gdf, scenario=scenario)
