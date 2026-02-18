"""Integration tests for the Google Earth Engine pipeline.

These tests use real GEE API calls and require:
  1. ``earthengine-api`` installed  (``uv pip install -e ".[gee]"``)
  2. A valid GEE-enabled Google Cloud project
  3. Prior authentication via ``earthengine authenticate``

Set the ``GEE_PROJECT_ID`` environment variable before running:

    GEE_PROJECT_ID=my-project uv run pytest tests/test_gee_pipeline.py -v
"""

import os

import pandas as pd
import pytest

# Skip the entire module if earthengine-api is not installed
ee = pytest.importorskip("ee", reason="earthengine-api not installed")

from climate_zarr.gee.client import initialize_gee, get_county_features
from climate_zarr.gee.config import GEEConfig, GEEPipelineConfig
from climate_zarr.gee.extract import build_variable_dataframe, extract_to_dataframe
from climate_zarr.gee.pipeline import run_gee_pipeline
from climate_zarr.gee.reducers import (
    reduce_precipitation,
    reduce_temperature,
    reduce_tasmax,
    reduce_tasmin,
)
from climate_zarr.transform import TARGET_COLUMNS

# ── Fixtures ──────────────────────────────────────────────────────────

PROJECT_ID = os.environ.get("GEE_PROJECT_ID", "")

pytestmark = pytest.mark.skipif(
    not PROJECT_ID,
    reason="GEE_PROJECT_ID environment variable not set",
)

TEST_MODEL = "NorESM2-LM"
TEST_SCENARIO = "ssp245"
TEST_YEAR = 2020
TEST_REGION = "conus"


@pytest.fixture(scope="module", autouse=True)
def gee_init():
    """Initialize GEE once for the entire test module."""
    initialize_gee(PROJECT_ID)


@pytest.fixture(scope="module")
def county_features():
    """Load CONUS county features (cached for the module)."""
    return get_county_features(region=TEST_REGION)


@pytest.fixture(scope="module")
def small_county_features():
    """Load a small subset of counties (Connecticut) for fast tests."""
    counties = get_county_features(region=TEST_REGION)
    # Filter to Connecticut (STATEFP == '09') for a small test set
    return counties.filter(ee.Filter.eq("STATEFP", "09"))


# ── Test GEE Initialization ──────────────────────────────────────────


class TestGEEInitialization:
    def test_gee_is_initialized(self):
        """Verify that GEE is initialized and can evaluate expressions."""
        result = ee.String("GEE_OK").getInfo()
        assert result == "GEE_OK"

    def test_cmip6_collection_exists(self):
        """Verify the NASA/GDDP-CMIP6 collection is accessible."""
        collection = ee.ImageCollection("NASA/GDDP-CMIP6")
        size = collection.limit(1).size().getInfo()
        assert size > 0


# ── Test County Features ─────────────────────────────────────────────


class TestCountyFeatures:
    def test_county_features_loaded(self, county_features):
        """Verify county features are loaded and non-empty."""
        count = county_features.size().getInfo()
        assert count > 3000, f"Expected >3000 CONUS counties, got {count}"

    def test_county_features_have_standard_properties(self, small_county_features):
        """Verify each feature has county_id, county_name, state."""
        first_feature = ee.Feature(small_county_features.first())
        properties = first_feature.getInfo()["properties"]

        assert "county_id" in properties
        assert "county_name" in properties
        assert "state" in properties

    def test_county_geoid_format(self, small_county_features):
        """Verify GEOIDs are 5-digit FIPS codes."""
        first_feature = ee.Feature(small_county_features.first())
        county_id = first_feature.get("county_id").getInfo()
        assert len(county_id) == 5
        assert county_id.isdigit()

    def test_connecticut_state_abbreviation(self, small_county_features):
        """Verify STATEFP -> state abbreviation mapping."""
        first_feature = ee.Feature(small_county_features.first())
        state = first_feature.get("state").getInfo()
        assert state == "CT"


# ── Test Single-Variable Reducers ────────────────────────────────────


class TestReducers:
    def test_reduce_precipitation(self, small_county_features):
        """Test precipitation reducer returns expected properties."""
        result_fc = reduce_precipitation(
            year=TEST_YEAR,
            model=TEST_MODEL,
            scenario=TEST_SCENARIO,
            counties=small_county_features,
        )
        dataframe = extract_to_dataframe(result_fc)

        assert not dataframe.empty
        assert "total_annual_precip_mm" in dataframe.columns or "mean" not in dataframe.columns
        assert "year" in dataframe.columns
        assert "scenario" in dataframe.columns
        assert "county_id" in dataframe.columns
        assert dataframe["year"].iloc[0] == TEST_YEAR
        assert dataframe["scenario"].iloc[0] == TEST_SCENARIO

    def test_reduce_temperature(self, small_county_features):
        """Test temperature reducer returns expected properties."""
        result_fc = reduce_temperature(
            year=TEST_YEAR,
            model=TEST_MODEL,
            scenario=TEST_SCENARIO,
            counties=small_county_features,
        )
        dataframe = extract_to_dataframe(result_fc)

        assert not dataframe.empty
        assert "county_id" in dataframe.columns
        assert "year" in dataframe.columns

    def test_reduce_tasmax(self, small_county_features):
        """Test tasmax reducer returns expected properties."""
        result_fc = reduce_tasmax(
            year=TEST_YEAR,
            model=TEST_MODEL,
            scenario=TEST_SCENARIO,
            counties=small_county_features,
        )
        dataframe = extract_to_dataframe(result_fc)

        assert not dataframe.empty
        assert "county_id" in dataframe.columns

    def test_reduce_tasmin(self, small_county_features):
        """Test tasmin reducer returns expected properties."""
        result_fc = reduce_tasmin(
            year=TEST_YEAR,
            model=TEST_MODEL,
            scenario=TEST_SCENARIO,
            counties=small_county_features,
        )
        dataframe = extract_to_dataframe(result_fc)

        assert not dataframe.empty
        assert "county_id" in dataframe.columns


# ── Test Per-Variable DataFrame Building ─────────────────────────────


class TestBuildVariableDataFrame:
    def test_build_precipitation_dataframe(self, small_county_features):
        """Test building a multi-year precipitation DataFrame."""
        dataframe = build_variable_dataframe(
            variable="pr",
            year_range=(2020, 2021),
            model=TEST_MODEL,
            scenario=TEST_SCENARIO,
            counties=small_county_features,
            batch_size=2,
        )

        assert not dataframe.empty
        assert "county_id" in dataframe.columns
        assert "year" in dataframe.columns
        assert "scenario" in dataframe.columns
        assert "total_annual_precip_mm" in dataframe.columns
        assert "days_above_threshold" in dataframe.columns

        # Should have data for 2 years
        assert dataframe["year"].nunique() == 2

    def test_build_temperature_dataframe(self, small_county_features):
        """Test building a temperature DataFrame."""
        dataframe = build_variable_dataframe(
            variable="tas",
            year_range=(2020, 2020),
            model=TEST_MODEL,
            scenario=TEST_SCENARIO,
            counties=small_county_features,
            batch_size=1,
        )

        assert not dataframe.empty
        assert "mean_annual_temp_c" in dataframe.columns


# ── Test Output Schema ───────────────────────────────────────────────


class TestOutputSchema:
    def test_merged_output_matches_target_columns(self, small_county_features):
        """Verify that the merged output has exactly the TARGET_COLUMNS."""
        from climate_zarr.transform import merge_climate_dataframes

        per_variable = {}
        for variable_name in ("pr", "tas", "tasmax", "tasmin"):
            per_variable[variable_name] = build_variable_dataframe(
                variable=variable_name,
                year_range=(2020, 2020),
                model=TEST_MODEL,
                scenario=TEST_SCENARIO,
                counties=small_county_features,
                batch_size=1,
            )

        merged = merge_climate_dataframes(per_variable)

        assert list(merged.columns) == TARGET_COLUMNS
        assert not merged.empty
        assert merged["cid2"].dtype == "Int64"
        assert merged["year"].dtype == "Int64"


# ── Test Full Pipeline ───────────────────────────────────────────────


class TestFullPipeline:
    def test_run_gee_pipeline_small(self, tmp_path):
        """End-to-end: run the GEE pipeline for 1 year, all variables."""
        config = GEEPipelineConfig(
            gee=GEEConfig(project_id=PROJECT_ID),
            models=[TEST_MODEL],
            scenarios=[TEST_SCENARIO],
            variables=("pr", "tas", "tasmax", "tasmin"),
            year_range=(2020, 2020),
            region=TEST_REGION,
            output_dir=tmp_path,
        )

        result = run_gee_pipeline(config)

        assert not result.merged_df.empty
        assert result.output_path is not None
        assert result.output_path.exists()
        assert list(result.merged_df.columns) == TARGET_COLUMNS

        # Verify CSV can be read back
        csv_dataframe = pd.read_csv(result.output_path)
        assert len(csv_dataframe) == len(result.merged_df)
