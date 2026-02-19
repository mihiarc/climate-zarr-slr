"""Server-side GEE computation for each climate variable.

Each reducer function takes a year, model, scenario, county FeatureCollection,
and GEE configuration, then returns an ``ee.FeatureCollection`` with
county-level annual statistics as properties.
"""

import ee

from climate_zarr.gee.client import get_cmip6_collection


def _reduce_with_centroid_backfill(
    image: ee.Image,
    counties: ee.FeatureCollection,
    scale: int,
    check_property: str,
) -> ee.FeatureCollection:
    """Run reduceRegions, then backfill null features by sampling at centroid.

    Small counties (e.g. Virginia independent cities) may not contain any
    pixel centers at the given scale.  For those features, we sample the
    image at the county centroid buffered by one pixel width so the nearest
    grid cell value is captured.

    Parameters
    ----------
    image : ee.Image
        The image to reduce (single- or multi-band).
    counties : ee.FeatureCollection
        County polygons to reduce over.
    scale : int
        Resolution in meters for ``reduceRegions``.
    check_property : str
        A property name produced by the reducer to check for null
        (e.g. ``"total_annual_precip_mm"`` or ``"mean"``).
    """
    reduced = image.reduceRegions(
        collection=counties,
        reducer=ee.Reducer.mean(),
        scale=scale,
    )

    has_data = reduced.filter(ee.Filter.notNull([check_property]))
    null_data = reduced.filter(ee.Filter.Not(ee.Filter.notNull([check_property])))

    def backfill_at_centroid(feature: ee.Feature) -> ee.Feature:
        centroid_buffer = feature.geometry().centroid(1).buffer(scale)
        sampled = image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=centroid_buffer,
            scale=scale,
        )
        return feature.set(sampled)

    backfilled = null_data.map(backfill_at_centroid)

    return has_data.merge(backfilled)


def reduce_precipitation(
    year: int,
    model: str,
    scenario: str,
    counties: ee.FeatureCollection,
    collection_id: str = "NASA/GDDP-CMIP6",
    scale: int = 27830,
) -> ee.FeatureCollection:
    """Compute annual precipitation statistics per county.

    Server-side operations:
    - Convert kg/m^2/s -> mm/day  (multiply by 86400)
    - Annual total precipitation  (sum of daily values)
    - Days above 25.4 mm (1 inch) threshold

    Returns
    -------
    ee.FeatureCollection
        Each feature carries ``county_id``, ``year``, ``scenario``,
        ``county_name``, ``state``, ``total_annual_precip_mm``,
        ``days_above_threshold``.
    """
    daily_collection = get_cmip6_collection(
        model, scenario, "pr", year, collection_id
    )

    # Convert kg/m2/s to mm/day (server-side)
    daily_mm = daily_collection.map(
        lambda image: image.multiply(86400).copyProperties(image, ["system:time_start"])
    )

    # Annual total: sum all daily values into one image
    annual_total_image = daily_mm.sum()

    # Days above 25.4 mm threshold: mark each day as 0/1 then sum
    threshold_image = daily_mm.map(
        lambda image: image.gt(25.4).rename("pr").copyProperties(image, ["system:time_start"])
    ).sum()

    # Combine into a two-band image for a single reduceRegions call
    combined_image = annual_total_image.rename("total_annual_precip_mm").addBands(
        threshold_image.rename("days_above_threshold")
    )

    reduced = _reduce_with_centroid_backfill(
        image=combined_image,
        counties=counties,
        scale=scale,
        check_property="total_annual_precip_mm",
    )

    # Stamp year and scenario onto every feature
    reduced = reduced.map(
        lambda feature: feature.set({"year": year, "scenario": scenario})
    )
    return reduced


def reduce_temperature(
    year: int,
    model: str,
    scenario: str,
    counties: ee.FeatureCollection,
    collection_id: str = "NASA/GDDP-CMIP6",
    scale: int = 27830,
) -> ee.FeatureCollection:
    """Compute annual mean temperature per county.

    Server-side: convert K -> C  (subtract 273.15), then annual mean.

    Returns
    -------
    ee.FeatureCollection
        Properties: ``county_id``, ``year``, ``scenario``, ``county_name``,
        ``state``, ``mean_annual_temp_c``.
    """
    daily_collection = get_cmip6_collection(
        model, scenario, "tas", year, collection_id
    )

    daily_celsius = daily_collection.map(
        lambda image: image.subtract(273.15).copyProperties(image, ["system:time_start"])
    )

    annual_mean_image = daily_celsius.mean().rename("mean_annual_temp_c")

    reduced = _reduce_with_centroid_backfill(
        image=annual_mean_image,
        counties=counties,
        scale=scale,
        check_property="mean",
    )

    reduced = reduced.map(
        lambda feature: feature.set({"year": year, "scenario": scenario})
    )
    return reduced


def reduce_tasmax(
    year: int,
    model: str,
    scenario: str,
    counties: ee.FeatureCollection,
    collection_id: str = "NASA/GDDP-CMIP6",
    scale: int = 27830,
) -> ee.FeatureCollection:
    """Compute annual tasmax statistics per county.

    Server-side: K -> C, then annual mean and count of days > 32.2 C (90 F).

    Returns
    -------
    ee.FeatureCollection
        Properties: ``county_id``, ``year``, ``scenario``, ``county_name``,
        ``state``, ``mean_annual_tasmax_c``, ``heat_index_days``.
    """
    daily_collection = get_cmip6_collection(
        model, scenario, "tasmax", year, collection_id
    )

    daily_celsius = daily_collection.map(
        lambda image: image.subtract(273.15).copyProperties(image, ["system:time_start"])
    )

    # Annual mean of daily max temperature
    annual_mean_image = daily_celsius.mean()

    # Days above 32.2 C (90 F)
    heat_days_image = daily_celsius.map(
        lambda image: image.gt(32.2).rename("tasmax").copyProperties(image, ["system:time_start"])
    ).sum()

    combined_image = annual_mean_image.rename("mean_annual_tasmax_c").addBands(
        heat_days_image.rename("heat_index_days")
    )

    reduced = _reduce_with_centroid_backfill(
        image=combined_image,
        counties=counties,
        scale=scale,
        check_property="mean_annual_tasmax_c",
    )

    reduced = reduced.map(
        lambda feature: feature.set({"year": year, "scenario": scenario})
    )
    return reduced


def reduce_tasmin(
    year: int,
    model: str,
    scenario: str,
    counties: ee.FeatureCollection,
    collection_id: str = "NASA/GDDP-CMIP6",
    scale: int = 27830,
) -> ee.FeatureCollection:
    """Compute annual tasmin statistics per county.

    Server-side: K -> C, then count of days < 0 C (freezing).

    Returns
    -------
    ee.FeatureCollection
        Properties: ``county_id``, ``year``, ``scenario``, ``county_name``,
        ``state``, ``cold_days``.
    """
    daily_collection = get_cmip6_collection(
        model, scenario, "tasmin", year, collection_id
    )

    daily_celsius = daily_collection.map(
        lambda image: image.subtract(273.15).copyProperties(image, ["system:time_start"])
    )

    # Days below 0 C (freezing)
    cold_days_image = daily_celsius.map(
        lambda image: image.lt(0).rename("tasmin").copyProperties(image, ["system:time_start"])
    ).sum().rename("cold_days")

    reduced = _reduce_with_centroid_backfill(
        image=cold_days_image,
        counties=counties,
        scale=scale,
        check_property="mean",
    )

    reduced = reduced.map(
        lambda feature: feature.set({"year": year, "scenario": scenario})
    )
    return reduced


# Map variable names to their reducer functions
VARIABLE_REDUCERS: dict[str, callable] = {
    "pr": reduce_precipitation,
    "tas": reduce_temperature,
    "tasmax": reduce_tasmax,
    "tasmin": reduce_tasmin,
}
