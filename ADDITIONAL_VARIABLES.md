# Additional Climate Variables - NEX-GDDP-CMIP6

**Date**: 2025-10-11
**Source**: NASA NEX-GDDP-CMIP6 NorESM2-LM

## Overview

Beyond the currently downloaded variables (pr, tas, tasmax, tasmin), NASA NEX-GDDP-CMIP6 provides additional climate variables that can enhance sea level rise vulnerability assessments.

## Available Additional Variables

### 1. Near-Surface Relative Humidity (hurs)

**Variable ID**: `hurs`
**Units**: % (percentage)
**Description**: Relative humidity at 2 meters above surface
**Temporal Resolution**: Daily
**Typical Range**: 0-100%

**SLR Research Applications**:
- **Heat Index Calculations**: Combined with temperature for thermal stress assessment
- **Indoor Climate Control**: Estimate cooling/dehumidification energy demands
- **Health Impacts**: Assess mold risk and respiratory health concerns in coastal communities
- **Agriculture**: Crop stress and disease vulnerability in coastal agricultural areas
- **Water Resources**: Evapotranspiration estimates for coastal water management

**Data Volume** (per scenario):
- **Files**: 86 (2015-2100)
- **Storage**: ~20 GB
- **Total for 4 scenarios**: ~80 GB

**Download URL**:
```
https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/
  NorESM2-LM/{scenario}/r1i1p1f1/hurs/
    hurs_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}.nc
```

---

### 2. Near-Surface Specific Humidity (huss)

**Variable ID**: `huss`
**Units**: kg/kg (kilograms of water vapor per kilogram of air)
**Description**: Specific humidity at 2 meters above surface
**Temporal Resolution**: Daily
**Typical Range**: 0.001-0.025 kg/kg

**SLR Research Applications**:
- **Atmospheric Water Content**: Understand moisture availability for extreme precipitation
- **Condensation Risk**: Building design considerations for coastal infrastructure
- **Comfort Indices**: Alternative to relative humidity for thermal comfort
- **Meteorological Studies**: More physically meaningful than relative humidity
- **Climate Model Validation**: Direct comparison with atmospheric observations

**Data Volume** (per scenario):
- **Files**: 86 (2015-2100)
- **Storage**: ~20 GB
- **Total for 4 scenarios**: ~80 GB

**Download URL**:
```
https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/
  NorESM2-LM/{scenario}/r1i1p1f1/huss/
    huss_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}.nc
```

---

### 3. Surface Downwelling Longwave Radiation (rlds)

**Variable ID**: `rlds`
**Units**: W/m² (watts per square meter)
**Description**: Thermal radiation from atmosphere to surface
**Temporal Resolution**: Daily
**Typical Range**: 200-500 W/m²

**SLR Research Applications**:
- **Urban Heat Island**: Nighttime cooling analysis in coastal cities
- **Building Energy**: Heating/cooling load calculations
- **Agricultural Productivity**: Net radiation balance for crop modeling
- **Infrastructure Thermal Stress**: Pavement and building material degradation
- **Coral Reef Health**: Ocean surface heat flux (indirect indicator)

**Data Volume** (per scenario):
- **Files**: 86 (2015-2100)
- **Storage**: ~20 GB
- **Total for 4 scenarios**: ~80 GB

**Download URL**:
```
https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/
  NorESM2-LM/{scenario}/r1i1p1f1/rlds/
    rlds_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}.nc
```

---

### 4. Surface Downwelling Shortwave Radiation (rsds)

**Variable ID**: `rsds`
**Units**: W/m² (watts per square meter)
**Description**: Solar radiation reaching Earth's surface
**Temporal Resolution**: Daily
**Typical Range**: 0-350 W/m² (daily average)

**SLR Research Applications**:
- **Solar Energy Potential**: Renewable energy planning for coastal resilience
- **Agricultural Productivity**: Photosynthesis and crop yield modeling
- **Building Energy**: Passive solar design and cooling loads
- **UV Exposure**: Public health implications for coastal populations
- **Evapotranspiration**: Water resource management
- **Ecosystem Modeling**: Primary productivity in coastal wetlands

**Data Volume** (per scenario):
- **Files**: 86 (2015-2100)
- **Storage**: ~20 GB
- **Total for 4 scenarios**: ~80 GB

**Download URL**:
```
https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/
  NorESM2-LM/{scenario}/r1i1p1f1/rsds/
    rsds_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}.nc
```

---

### 5. Near-Surface Wind Speed (sfcWind)

**Variable ID**: `sfcWind`
**Units**: m/s (meters per second)
**Description**: Wind speed at 10 meters above surface
**Temporal Resolution**: Daily
**Typical Range**: 0-20 m/s (extreme events can exceed)

**SLR Research Applications**:
- **Storm Surge Modeling**: Critical input for coastal flood risk assessment ⭐
- **Wind Energy Potential**: Offshore wind farm planning
- **Coastal Erosion**: Beach and dune erosion risk assessment ⭐
- **Infrastructure Design**: Wind load calculations for coastal structures
- **Navigation Safety**: Marine transportation and port operations
- **Wildfire Risk**: Coastal vegetation fire spread (when combined with precipitation)
- **Building Ventilation**: Natural cooling potential in coastal buildings

**Data Volume** (per scenario):
- **Files**: 86 (2015-2100)
- **Storage**: ~20 GB
- **Total for 4 scenarios**: ~80 GB

**Download URL**:
```
https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/
  NorESM2-LM/{scenario}/r1i1p1f1/sfcWind/
    sfcWind_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}.nc
```

---

## Priority Recommendations for SLR Research

### High Priority ⭐⭐⭐

**1. sfcWind (Near-Surface Wind Speed)**
- **Why**: Essential for storm surge modeling and coastal erosion assessment
- **Use Case**: Identify counties with increasing wind-driven flood risk
- **Analysis**: Calculate changes in extreme wind events (e.g., 95th percentile)

**2. rsds (Solar Radiation)**
- **Why**: Useful for understanding evapotranspiration and water stress
- **Use Case**: Assess drought vulnerability in coastal agricultural counties
- **Analysis**: Annual solar radiation trends affecting water availability

### Medium Priority ⭐⭐

**3. hurs (Relative Humidity)**
- **Why**: Important for heat stress and health impact assessments
- **Use Case**: Combined with temperature for heat index calculations
- **Analysis**: Days with dangerous heat-humidity combinations

**4. rlds (Longwave Radiation)**
- **Why**: Complements temperature data for energy balance
- **Use Case**: Building energy demand projections
- **Analysis**: Nighttime cooling potential in urban coastal areas

### Lower Priority ⭐

**5. huss (Specific Humidity)**
- **Why**: More scientifically rigorous than relative humidity, but overlaps with hurs
- **Use Case**: Atmospheric moisture content for extreme precipitation analysis
- **Analysis**: Trends in atmospheric water vapor capacity

---

## Storage Requirements Summary

### Current Dataset
- **4 variables × 4 scenarios**: 1,377 files, ~310 GB

### With All Additional Variables
- **9 variables × 4 scenarios**: 3,096 files, ~720 GB
- **Incremental addition**: 1,719 files, ~410 GB

### Recommended Priority Download
- **6 variables (add sfcWind + rsds)**: 2,064 files, ~490 GB
- **Incremental addition**: 687 files, ~180 GB

---

## Download Instructions

### Option 1: Individual File Download (curl)

```bash
# Download single year for a variable
curl -O "https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/NorESM2-LM/ssp245/r1i1p1f1/sfcWind/sfcWind_day_NorESM2-LM_ssp245_r1i1p1f1_gn_2050.nc"
```

### Option 2: Bulk Download Script

Create a download script for a specific variable and scenario:

```bash
#!/bin/bash
# Download all years for sfcWind/ssp245

VARIABLE="sfcWind"
SCENARIO="ssp245"
OUTPUT_DIR="/Volumes/SSD1TB/NorESM2-LM/$VARIABLE/$SCENARIO"

mkdir -p "$OUTPUT_DIR"

for YEAR in {2015..2100}; do
  FILE="${VARIABLE}_day_NorESM2-LM_${SCENARIO}_r1i1p1f1_gn_${YEAR}.nc"
  URL="https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/NorESM2-LM/${SCENARIO}/r1i1p1f1/${VARIABLE}/${FILE}"

  echo "Downloading $YEAR..."
  curl -o "$OUTPUT_DIR/$FILE" "$URL"

  # Brief pause to avoid overwhelming server
  sleep 1
done

echo "Download complete: $VARIABLE/$SCENARIO"
```

### Option 3: Python with Parallel Downloads

```python
from concurrent.futures import ThreadPoolExecutor
import requests
from pathlib import Path

def download_file(url, output_path):
    """Download a single file."""
    response = requests.get(url, stream=True)
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return output_path

def download_variable(variable, scenario, output_base):
    """Download all years for a variable/scenario."""
    output_dir = Path(output_base) / variable / scenario
    output_dir.mkdir(parents=True, exist_ok=True)

    base_url = "https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6"

    urls = []
    for year in range(2015, 2101):
        filename = f"{variable}_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}.nc"
        url = f"{base_url}/NorESM2-LM/{scenario}/r1i1p1f1/{variable}/{filename}"
        output_path = output_dir / filename
        urls.append((url, output_path))

    # Download in parallel (10 concurrent downloads)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(download_file, url, path) for url, path in urls]
        for i, future in enumerate(futures, 1):
            future.result()
            print(f"Downloaded {i}/{len(urls)}: {variable}/{scenario}")

# Example: Download sfcWind for ssp245
download_variable("sfcWind", "ssp245", "/Volumes/SSD1TB/NorESM2-LM")
```

---

## Integration into Existing Workflow

### 1. Modify Stage 1 Script

Update `scripts/process_scenario_stage1.sh` to include additional variables:

```bash
# Add sfcWind and rsds to the variable list
for VAR in pr tas tasmax tasmin sfcWind rsds; do
  for REGION in conus alaska hawaii puerto_rico guam; do
    # ... existing conversion code ...
  done
done
```

### 2. Create New Processor Classes

Add specialized processors for the new variables:

```python
# src/climate_zarr/processors/wind_processor.py
from .base_processor import BaseCountyProcessor

class WindProcessor(BaseCountyProcessor):
    """Process wind speed data for county statistics."""

    def calculate_statistics(self, data, county_id):
        """Calculate wind-specific statistics."""
        return {
            'wind_mean': float(data.mean()),
            'wind_max': float(data.max()),
            'wind_95th': float(data.quantile(0.95)),
            'days_above_15ms': int((data > 15).sum()),  # High wind days
        }
```

### 3. Update Configuration

Modify `climate_config.py` to include new variables:

```python
class ClimateVariable(str, Enum):
    PRECIPITATION = "pr"
    TEMPERATURE = "tas"
    MAX_TEMPERATURE = "tasmax"
    MIN_TEMPERATURE = "tasmin"
    WIND_SPEED = "sfcWind"      # NEW
    SOLAR_RADIATION = "rsds"    # NEW
    # ... add others as needed
```

---

## Analysis Examples

### Wind Speed Trends for Storm Surge Assessment

```python
import xarray as xr
import pandas as pd

# Open wind speed Zarr store
ds = xr.open_zarr('/Volumes/SSD1TB/climate_outputs/zarr/sfcWind/conus/ssp245/conus_ssp245_sfcWind_daily.zarr')

# Calculate extreme wind statistics by decade
decades = ds.groupby(ds.time.dt.year // 10 * 10)

extreme_wind = decades.map(lambda x: x.sfcWind.quantile(0.99, dim='time'))

# Identify counties with increasing extreme wind trends
trend = extreme_wind.polyfit(dim='time', deg=1)
high_risk_counties = trend.where(trend.polyfit_coefficients[0] > threshold)
```

### Solar Radiation for Agricultural Vulnerability

```python
# Calculate annual solar radiation totals
annual_solar = ds.rsds.resample(time='1Y').sum()

# Identify areas with declining solar (increased cloudiness)
solar_trend = annual_solar.polyfit(dim='time', deg=1)
declining_areas = solar_trend.where(solar_trend.polyfit_coefficients[0] < 0)
```

---

## Cost-Benefit Analysis

### Immediate Value Variables

| Variable | Storage | Download Time* | Research Value |
|----------|---------|----------------|----------------|
| sfcWind | 80 GB | ~4-6 hours | ⭐⭐⭐ High (storm surge) |
| rsds | 80 GB | ~4-6 hours | ⭐⭐ Medium (water stress) |
| hurs | 80 GB | ~4-6 hours | ⭐⭐ Medium (heat stress) |
| rlds | 80 GB | ~4-6 hours | ⭐ Low (energy modeling) |
| huss | 80 GB | ~4-6 hours | ⭐ Low (overlaps with hurs) |

*Assuming 5 MB/s average download speed

### Recommended Approach

**Phase 1** (Immediate):
1. Download `sfcWind` for all 4 scenarios (~80 GB)
2. Validate data quality
3. Create county-level wind statistics
4. Integrate into existing analysis pipeline

**Phase 2** (Next):
1. Add `rsds` for water stress analysis (~80 GB)
2. Combine with precipitation and temperature for comprehensive drought assessment

**Phase 3** (Optional):
1. Add humidity variables if heat stress analysis is prioritized
2. Add longwave radiation for energy modeling studies

---

## Next Steps

1. **Review Priority List**: Confirm which variables align with research goals
2. **Storage Check**: Verify available disk space on external drive
3. **Download**: Start with high-priority variables (sfcWind recommended)
4. **Validation**: Test data quality before bulk processing
5. **Integration**: Extend processing pipeline to include new variables
6. **Analysis**: Develop county-level metrics for new variables

---

**Last Updated**: 2025-10-11
**Contact**: See DATA_ORGANIZATION.md for data source citations
