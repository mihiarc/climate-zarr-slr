"""Pydantic v2 configuration models for the Google Earth Engine pipeline."""

from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from climate_zarr.climate_config import get_config

SUPPORTED_VARIABLES = ("pr", "tas", "tasmax", "tasmin")


class GEEConfig(BaseModel):
    """Google Earth Engine authentication and project settings."""

    project_id: str = Field(description="Google Cloud project ID for GEE access")
    collection_id: str = Field(
        default="NASA/GDDP-CMIP6",
        description="GEE ImageCollection ID for CMIP6 data",
    )
    scale: int = Field(
        default=27830,
        description="Processing resolution in meters (~0.25 degree grid)",
    )
    county_asset: str = Field(
        default="TIGER/2018/Counties",
        description="GEE FeatureCollection ID for US county boundaries",
    )
    batch_size: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Years per batch to avoid GEE timeouts",
    )


class GEEPipelineConfig(BaseModel):
    """Full pipeline configuration for the GEE data source."""

    gee: GEEConfig = Field(description="GEE-specific settings")
    models: list[str] = Field(
        default=["NorESM2-LM"],
        description="CMIP6 models to process",
    )
    variables: tuple = Field(
        default=("pr", "tas", "tasmax", "tasmin"),
        description="Climate variables to process",
    )
    scenarios: list[str] = Field(
        default=["ssp245"],
        description="SSP scenarios to process (GEE has historical, ssp245, ssp585)",
    )
    year_range: tuple[int, int] = Field(
        default=(2015, 2100),
        description="Inclusive year range (start, end)",
    )
    region: str = Field(
        default="conus",
        description="Geographic region for county filtering",
    )
    output_dir: Path = Field(
        default=Path("climate_outputs"),
        description="Base output directory",
    )
    output_file: Optional[Path] = Field(
        default=None,
        description="Final CSV path; defaults to {output_dir}/gee/{region}_{scenario}_climate_stats.csv",
    )

    @field_validator("region")
    @classmethod
    def validate_region(cls, region_value: str) -> str:
        config = get_config()
        if region_value.lower() not in config.regions:
            available_regions = list(config.regions.keys())
            raise ValueError(
                f"Unknown region '{region_value}'. Available: {available_regions}"
            )
        return region_value.lower()

    @field_validator("variables", mode="before")
    @classmethod
    def validate_variables(cls, variables_value) -> tuple:
        variables_tuple = tuple(variables_value)
        for variable_name in variables_tuple:
            if variable_name not in SUPPORTED_VARIABLES:
                raise ValueError(
                    f"Unsupported variable '{variable_name}'. "
                    f"Supported: {SUPPORTED_VARIABLES}"
                )
        return variables_tuple

    @field_validator("year_range")
    @classmethod
    def validate_year_range(cls, year_range_value: tuple[int, int]) -> tuple[int, int]:
        start_year, end_year = year_range_value
        if start_year > end_year:
            raise ValueError(
                f"start_year ({start_year}) must be <= end_year ({end_year})"
            )
        if start_year < 1950 or end_year > 2100:
            raise ValueError("year_range must be within 1950-2100")
        return year_range_value

    @model_validator(mode="after")
    def set_defaults(self) -> "GEEPipelineConfig":
        if self.output_file is None:
            scenario_label = "_".join(self.scenarios)
            self.output_file = (
                self.output_dir
                / "gee"
                / f"{self.region}_{scenario_label}_climate_stats.csv"
            )
        return self
