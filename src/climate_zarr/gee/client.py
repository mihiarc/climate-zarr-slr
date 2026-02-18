"""GEE connection, authentication, and data access helpers."""

import ee
from rich.console import Console

from climate_zarr.climate_config import get_config

console = Console()

# FIPS state codes to two-letter abbreviations (matches base_processor.py)
STATE_FIPS_TO_ABBR: dict[str, str] = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR",
    "78": "VI",
}

# CONUS state FIPS codes (excludes territories and non-contiguous states)
CONUS_STATE_FIPS: set[str] = {
    fips for fips, abbr in STATE_FIPS_TO_ABBR.items()
    if abbr not in {"AK", "HI", "AS", "GU", "MP", "PR", "VI"}
}


def initialize_gee(project_id: str) -> None:
    """Authenticate (if needed) and initialize the Earth Engine API.

    Parameters
    ----------
    project_id : str
        Google Cloud project with Earth Engine enabled.
    """
    try:
        ee.Initialize(project=project_id)
        console.print(f"[green]GEE initialized with project '{project_id}'[/green]")
    except Exception:
        console.print("[yellow]GEE not initialized, attempting authentication...[/yellow]")
        ee.Authenticate()
        ee.Initialize(project=project_id)
        console.print(f"[green]GEE authenticated and initialized with project '{project_id}'[/green]")


def get_cmip6_collection(
    model: str,
    scenario: str,
    variable: str,
    year: int,
    collection_id: str = "NASA/GDDP-CMIP6",
) -> ee.ImageCollection:
    """Return a filtered CMIP6 ImageCollection for one model/scenario/variable/year.

    Parameters
    ----------
    model : str
        CMIP6 model name (e.g. ``"NorESM2-LM"``).
    scenario : str
        SSP scenario or ``"historical"``.
    variable : str
        Climate variable band name (``pr``, ``tas``, ``tasmax``, ``tasmin``).
    year : int
        Calendar year.
    collection_id : str
        GEE asset path for the collection.

    Returns
    -------
    ee.ImageCollection
        Filtered to the requested slice, selecting only *variable*.
    """
    start_date = f"{year}-01-01"
    end_date = f"{year + 1}-01-01"

    collection = (
        ee.ImageCollection(collection_id)
        .filter(ee.Filter.eq("model", model))
        .filter(ee.Filter.eq("scenario", scenario))
        .filter(ee.Filter.date(start_date, end_date))
        .select(variable)
    )
    return collection


def get_county_features(
    region: str,
    county_asset: str = "TIGER/2018/Counties",
) -> ee.FeatureCollection:
    """Load TIGER/Line county boundaries, filtered by region.

    Adds ``county_id``, ``county_name``, and ``state`` properties to each
    feature so downstream code can build DataFrames that match the existing
    pipeline schema.

    Parameters
    ----------
    region : str
        Region key from ``ClimateConfig.regions`` (e.g. ``"conus"``).
    county_asset : str
        GEE FeatureCollection asset path.

    Returns
    -------
    ee.FeatureCollection
        County features with standardized properties.
    """
    climate_config = get_config()
    region_config = climate_config.get_region(region)

    counties = ee.FeatureCollection(county_asset)

    # Use STATEFP filtering instead of filterBounds wherever possible.
    # GEE's filterBounds duplicates features that straddle spatial-index
    # tile boundaries (observed for 18 Wisconsin counties) and drops
    # features whose geometry extends beyond the bbox (6 south FL/TX counties).
    _REGION_FIPS: dict[str, set[str]] = {
        "conus": CONUS_STATE_FIPS,
        "alaska": {"02"},
        "hawaii": {"15"},
        "guam": {"66", "69"},
        "puerto_rico": {"72", "78"},
    }

    if region in _REGION_FIPS:
        fips_set = _REGION_FIPS[region]
        counties = counties.filter(
            ee.Filter.inList("STATEFP", ee.List(sorted(fips_set)))
        )
    else:
        # For "global" or unknown regions, fall back to bounding-box filter
        region_geometry = ee.Geometry.Rectangle(
            [region_config.lon_min, region_config.lat_min,
             region_config.lon_max, region_config.lat_max]
        )
        counties = counties.filterBounds(region_geometry)

    # Map standardized properties onto each feature
    state_fips_dict = ee.Dictionary(STATE_FIPS_TO_ABBR)

    def add_standard_properties(feature: ee.Feature) -> ee.Feature:
        geoid = feature.get("GEOID")
        county_name = feature.get("NAME")
        state_fips = feature.get("STATEFP")
        state_abbr = state_fips_dict.get(state_fips, "")
        return feature.set({
            "county_id": geoid,
            "county_name": county_name,
            "state": state_abbr,
        })

    counties = counties.map(add_standard_properties)

    console.print(
        f"[green]Loaded county features for region '{region}' "
        f"(bounds: {region_config.lon_min},{region_config.lat_min} to "
        f"{region_config.lon_max},{region_config.lat_max})[/green]"
    )
    return counties
