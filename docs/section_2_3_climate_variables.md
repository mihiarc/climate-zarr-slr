# Section 2.3: Climate Variables

## Manuscript Version (~230 words)

We incorporate four climate measures following Fan et al. (2018): (1) annual count of days with maximum temperature exceeding 90°F (32.2°C), (2) annual count of days with maximum temperature below 32°F (0°C), (3) annual count of days with precipitation exceeding 1 inch (25.4 mm), and (4) annual average mean daily temperature. These variables capture both chronic climate conditions and acute weather extremes relevant to population and economic outcomes.

Climate data are drawn from the NASA Earth Exchange Global Daily Downscaled Projections (NEX-GDDP-CMIP6) dataset (Thrasher et al., 2022), which provides bias-corrected and statistically downscaled daily outputs at 0.25° × 0.25° spatial resolution (approximately 25 km). We use projections from the Norwegian Earth System Model (NorESM2-LM), developed by the Norwegian Climate Center (Seland et al., 2020). The historical period spans 1950–2014, with future projections extending through 2100 under all four Shared Socioeconomic Pathways: SSP1-2.6, SSP2-4.5, SSP3-7.0, and SSP5-8.5.

Daily gridded data are aggregated to county-year observations through geometric clipping. For each county, we extract all grid cells intersecting the county boundary and compute the unweighted spatial mean across cells. Temporal aggregation follows the calendar year (January–December), with threshold exceedances counted daily before annual summation.

Climate projections cover all 50 U.S. states. However, Puerto Rico and the U.S. Virgin Islands lack coverage in NEX-GDDP-CMIP6, requiring alternative modeling approaches for these territories as discussed in Section [X.X].

---

## Detailed Technical Documentation

### 2.3.1 Variable Selection and Rationale

We incorporate four climate measures following the empirical framework established by Fan et al. (2018), who demonstrated their predictive power for migration and regional economic outcomes:

| Variable | Description | Threshold | Raw Units | Processed Units |
|----------|-------------|-----------|-----------|-----------------|
| Hot Days | Annual count of days with daily maximum temperature > 90°F | 32.2°C | Kelvin | Days/year |
| Cold Days | Annual count of days with daily minimum temperature < 32°F | 0°C | Kelvin | Days/year |
| Heavy Precipitation Days | Annual count of days with precipitation > 1 inch | 25.4 mm | kg/m²/s | Days/year |
| Mean Temperature | Annual average of daily mean temperature | N/A | Kelvin | °C |

#### Variable Definitions

**Hot Days (tasmax > 90°F / 32.2°C)**
- Source variable: `tasmax` (daily maximum near-surface air temperature)
- Captures extreme heat exposure relevant to health outcomes, labor productivity, and energy demand
- The 90°F threshold is a standard benchmark in climate-health literature

**Cold Days (tasmin < 32°F / 0°C)**
- Source variable: `tasmin` (daily minimum near-surface air temperature)
- Captures freezing conditions relevant to agriculture, infrastructure damage, and heating demand
- The 32°F threshold corresponds to the freezing point of water

**Heavy Precipitation Days (pr > 1 inch / 25.4 mm)**
- Source variable: `pr` (precipitation flux)
- Captures intense rainfall events associated with flooding, infrastructure stress, and agricultural impacts
- The 1-inch threshold is commonly used in hydrological and agricultural studies

**Annual Mean Temperature (tas)**
- Source variable: `tas` (daily mean near-surface air temperature)
- Captures chronic temperature conditions affecting long-term adaptation decisions
- Computed as the arithmetic mean of all daily values within a calendar year

### 2.3.2 Source Data: NASA NEX-GDDP-CMIP6

#### Dataset Overview

The NASA Earth Exchange Global Daily Downscaled Projections (NEX-GDDP-CMIP6) dataset provides high-resolution climate projections derived from the Coupled Model Intercomparison Project Phase 6 (CMIP6) archive. The dataset was developed by NASA's Earth Exchange team at Ames Research Center (Thrasher et al., 2022).

**Key Specifications:**
- **Spatial Resolution:** 0.25° × 0.25° (~25 km at the equator)
- **Temporal Resolution:** Daily
- **Historical Period:** 1950–2014
- **Projection Period:** 2015–2100
- **Downscaling Method:** Bias Correction Spatial Disaggregation (BCSD), daily variant
- **Reference Observations:** Global Meteorological Forcing Dataset (GMFD)

#### Climate Model: NorESM2-LM

We selected the Norwegian Earth System Model version 2, low-resolution configuration (NorESM2-LM), developed by the Norwegian Climate Center consortium including the Norwegian Meteorological Institute, NORCE Norwegian Research Centre, and Norwegian universities (Seland et al., 2020).

**Model Characteristics:**
- **Atmospheric Resolution:** ~2° horizontal
- **Ocean Resolution:** ~1° horizontal
- **Foundation:** Based on Community Earth System Model version 2.1 (CESM2.1)
- **Ocean Component:** Bergen Layered Ocean Model (BLOM) with iHAMOCC biogeochemistry
- **Equilibrium Climate Sensitivity:** 2.5 K (150-year estimate)

**CMIP6 Historical Forcing (1850–2014):**
- Time-dependent greenhouse gas concentrations
- Anthropogenic and natural aerosol emissions
- Land use and land cover changes
- Solar irradiance variations
- Volcanic aerosol forcing

#### Shared Socioeconomic Pathways (SSPs)

We utilize all four Tier-1 SSP scenarios to capture the full range of plausible climate futures:

| Scenario | Description | 2100 Warming (NorESM2-LM) | Radiative Forcing |
|----------|-------------|---------------------------|-------------------|
| SSP1-2.6 | Sustainability pathway; aggressive mitigation | +1.3 K | 2.6 W/m² |
| SSP2-4.5 | Middle-of-the-road; moderate mitigation | +2.2 K | 4.5 W/m² |
| SSP3-7.0 | Regional rivalry; limited mitigation | +3.0 K | 7.0 W/m² |
| SSP5-8.5 | Fossil-fueled development; no mitigation | +3.9 K | 8.5 W/m² |

*Note: Warming values are relative to 1850–1879 baseline, computed for 2090–2099.*

### 2.3.3 Aggregation Methodology

#### Spatial Aggregation: Geometric Clipping

Daily gridded climate data are aggregated to U.S. county boundaries using a geometric clipping approach implemented with the `rioxarray` library. The process follows these steps:

1. **Coordinate System Alignment**
   - Climate data CRS: EPSG:4326 (WGS84)
   - County boundary CRS: EPSG:4326 (WGS84)
   - Automatic CRS transformation if misaligned

2. **Geometric Clipping**
   - For each county polygon, extract all grid cells that intersect the boundary
   - Use `all_touched=True` parameter to include partial cells (critical for coastal counties)
   - This effectively captures all pixels whose centers or edges touch the county boundary

3. **Spatial Mean Calculation**
   - Compute unweighted arithmetic mean across all extracted grid cells
   - NaN values at boundaries are excluded via `skipna=True`
   - Result: One daily value per county representing the county-wide average

```python
# Core spatial aggregation logic (from processing_strategies.py)
daily_means = year_data.mean(dim=["y", "x"], skipna=True).values
```

**Important Notes:**
- This approach produces an **unweighted spatial mean**, treating all grid cells equally regardless of the fraction of the cell within the county boundary
- For small counties that span only 1-2 grid cells, this may introduce spatial sampling uncertainty
- Coastal counties receive special handling with `all_touched=True` to ensure offshore pixels are not incorrectly included

#### Temporal Aggregation: Calendar Year

Climate statistics are aggregated on a **calendar year** basis (January 1 – December 31):

1. **Daily Processing**
   - Extract year from timestamp using pandas datetime parsing
   - Group daily observations by calendar year

2. **Annual Statistics**
   - **Threshold exceedances:** Count days meeting threshold criteria, then sum annually
   - **Mean temperature:** Arithmetic mean of all valid daily values within the year

```python
# Year extraction logic (from spatial_utils.py)
years = pd.to_datetime(time_values).year.values
unique_years = np.unique(years)
```

#### Unit Conversions

Raw NEX-GDDP-CMIP6 data require unit conversions before analysis:

| Variable | Raw Units | Conversion | Final Units |
|----------|-----------|------------|-------------|
| tasmax, tasmin, tas | Kelvin (K) | Subtract 273.15 | Celsius (°C) |
| pr | kg/m²/s | Multiply by 86,400 | mm/day |

```python
# Temperature conversion (from data_utils.py)
celsius = kelvin - 273.15

# Precipitation conversion (from data_utils.py)
mm_per_day = kg_m2_s * 86400  # 86,400 seconds per day
```

### 2.3.4 Geographic Coverage

#### Supported Regions

The processing pipeline supports the following U.S. regions:

| Region | Bounding Box | County Count |
|--------|--------------|--------------|
| CONUS | 24.0°N–50.0°N, 125.0°W–66.0°W | ~3,100 |
| Alaska | 54.0°N–72.0°N, 180.0°W–129.0°W | ~30 |
| Hawaii | 18.0°N–29.0°N, 179.0°W–154.0°W | 5 |
| Puerto Rico | 17.5°N–18.6°N, 68.0°W–64.5°W | 78 |
| Guam/CNMI | 13.0°N–21.0°N, 144.0°E–146.0°E | <10 |

#### Coverage Limitations

**Puerto Rico and U.S. Virgin Islands:**
The NEX-GDDP-CMIP6 dataset does not include coverage for Puerto Rico or the U.S. Virgin Islands. This limitation affects approximately 78 Puerto Rico municipios and 3 USVI districts. Alternative approaches for these territories include:
- Use of regional climate models with Caribbean coverage
- Statistical downscaling of coarser GCM outputs
- Extrapolation from nearby covered regions (with appropriate uncertainty quantification)

**Small Island Territories:**
Guam, American Samoa, and Northern Mariana Islands have limited or no coverage in most global climate datasets due to their small spatial extent relative to typical GCM grid resolution.

### 2.3.5 Output Data Structure

Processed county-year climate statistics are output as CSV files with the following structure:

```
climate_outputs/
└── stats/
    └── {variable}/
        └── {region}/
            └── {scenario}/
                └── {region}_{scenario}_{variable}_stats.csv
```

**Output Columns (example for tasmax):**

| Column | Description |
|--------|-------------|
| year | Calendar year |
| scenario | Climate scenario (historical, ssp126, etc.) |
| county_id | FIPS code (GEOID) |
| county_name | County name |
| state | State abbreviation |
| mean_annual_tasmax_c | Annual mean of daily maximum temperature (°C) |
| days_above_threshold_c | Count of days exceeding threshold |
| threshold_temp_c | Threshold value used (°C) |

---

## References

### Primary Citations

- Fan, Q., Fisher-Vanden, K., & Klaiber, H. A. (2018). Climate change, migration, and regional economic impacts in the United States. *Journal of the Association of Environmental and Resource Economists*, 5(3), 643–671. https://doi.org/10.1086/697168

- Thrasher, B., Wang, W., Michaelis, A., Melton, F., Lee, T., & Nemani, R. (2022). NASA Global Daily Downscaled Projections, CMIP6. *Scientific Data*, 9, 262. https://doi.org/10.1038/s41597-022-01393-4

- Seland, Ø., Bentsen, M., Olivié, D., Toniazzo, T., Gjermundsen, A., Graff, L. S., ... & Schulz, M. (2020). Overview of the Norwegian Earth System Model (NorESM2) and key climate response of CMIP6 DECK, historical, and scenario simulations. *Geoscientific Model Development*, 13(12), 6165–6200. https://doi.org/10.5194/gmd-13-6165-2020

### Data Sources

- NASA NEX-GDDP-CMIP6: https://www.nccs.nasa.gov/services/data-collections/land-based-products/nex-gddp-cmip6
- AWS S3 Access: s3://nex-gddp-cmip6
- Google Earth Engine: https://developers.google.com/earth-engine/datasets/catalog/NASA_GDDP-CMIP6

### Software Implementation

- Processing pipeline: `climate-zarr-slr` (this repository)
- Key dependencies: xarray, rioxarray, geopandas, pandas, numpy
- Spatial operations: rioxarray.clip() with geometric boundaries

---

*Document generated: 2025-01-04*
*Source: climate-zarr-slr codebase analysis*
