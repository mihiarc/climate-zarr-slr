# Climate Data Organization

## Overview

This document describes the organization of climate data for the **climate-zarr-slr** project, a specialized pipeline for studying the impact of climate change on population and income in a sea level rise (SLR) context.

## Storage Architecture

All climate data is stored on an **external SSD drive** (`/Volumes/SSD1TB/`) and accessed through **symbolic links** to avoid duplicating large datasets in the project directory.

### Why External Storage?

- **Size**: Raw NetCDF data is ~14GB per variable for historical data alone
- **Performance**: External SSD provides fast read/write for large datasets
- **Portability**: Data can be shared across machines or projects
- **Git-Friendly**: Data files are never committed to version control

## Data Directory Structure

```
climate-zarr-slr/
├── data/                               # Symbolic links to external data
│   ├── raw_netcdf -> /Volumes/SSD1TB/NorESM2-LM/
│   ├── climate_outputs -> /Volumes/SSD1TB/climate_outputs/
│   ├── pr_historical_conus.zarr -> /Volumes/SSD1TB/pr_historical_conus.zarr
│   ├── tas_historical_conus.zarr -> /Volumes/SSD1TB/tas_historical_conus.zarr
│   ├── tasmax_historical_conus.zarr -> /Volumes/SSD1TB/tasmax_historical_conus.zarr
│   └── tasmin_historical_conus.zarr -> /Volumes/SSD1TB/tasmin_historical_conus.zarr
└── regional_counties/                  # US Census county shapefiles
    ├── alaska_counties.*
    ├── conus_counties.*
    ├── guam_counties.*
    ├── hawaii_counties.*
    └── puerto_rico_counties.*
```

## Raw NetCDF Data Structure

### Location
`/Volumes/SSD1TB/NorESM2-LM/`

### Climate Model
**NorESM2-LM** (Norwegian Earth System Model - Low Resolution)
- Part of CMIP6 (Coupled Model Intercomparison Project Phase 6)
- Model developed by Norwegian Climate Center

### Directory Structure
```
NorESM2-LM/
├── pr/                    # Precipitation
│   ├── historical/        # 1950-2014
│   ├── ssp126/           # Low emissions scenario
│   ├── ssp245/           # Medium emissions scenario
│   ├── ssp370/           # Medium-high emissions scenario
│   └── ssp585/           # High emissions scenario
├── tas/                   # Air Temperature
│   └── [same scenarios]
├── tasmax/                # Daily Maximum Temperature
│   └── [same scenarios]
├── tasmin/                # Daily Minimum Temperature
│   └── [same scenarios]
├── hurs/                  # Relative Humidity
│   └── [same scenarios]
└── sfcWind/               # Surface Wind Speed
    └── [same scenarios]
```

### Variables

| Variable | Description | Units | Temporal Resolution |
|----------|-------------|-------|---------------------|
| `pr` | Precipitation | kg m⁻² s⁻¹ (mm/day) | Daily |
| `tas` | Near-Surface Air Temperature | K (converted to °C) | Daily |
| `tasmax` | Daily Maximum Temperature | K (converted to °C) | Daily |
| `tasmin` | Daily Minimum Temperature | K (converted to °C) | Daily |
| `hurs` | Relative Humidity | % | Daily |
| `sfcWind` | Surface Wind Speed | m s⁻¹ | Daily |

### File Naming Convention
```
{var}_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}_v1.1.nc

Example: pr_day_NorESM2-LM_historical_r1i1p1f1_gn_1950_v1.1.nc
```

- `{var}`: Variable name (pr, tas, tasmax, tasmin, etc.)
- `{scenario}`: Climate scenario (historical, ssp126, ssp245, ssp370, ssp585)
- `r1i1p1f1`: Realization/initialization/physics/forcing variant
- `gn`: Global native grid
- `{year}`: Year of data (1950-2100)
- `v1.1`: Version

### Data Size
- **Historical pr**: ~14GB (1950-2014, 65 years × daily data)
- **Historical tas/tasmax/tasmin**: Similar sizes
- **Future scenarios**: Similar or larger (2015-2100, 86 years)

### Time Coverage
- **Historical**: 1950-2014
- **SSP Scenarios**: 2015-2100

## Processed Data Outputs

### Location
`/Volumes/SSD1TB/climate_outputs/`

### Directory Structure
```
climate_outputs/
├── zarr/                          # Converted Zarr datasets
│   ├── pr/
│   │   ├── alaska/
│   │   │   ├── historical/        # pr_historical_alaska.zarr
│   │   │   └── ssp370/
│   │   ├── conus/
│   │   │   ├── historical/        # pr_historical_conus.zarr (~956MB)
│   │   │   └── ssp370/
│   │   ├── guam/
│   │   ├── hawaii/
│   │   └── puerto_rico/
│   ├── tas/                       # Same structure
│   ├── tasmax/                    # Same structure
│   └── tasmin/                    # Same structure
│
├── stats/                         # County-level statistics (CSV)
│   ├── pr/
│   │   ├── alaska/
│   │   │   └── historical/
│   │   │       ├── pr_stats_alaska_historical.csv
│   │   │       └── pr_stats_alaska_historical.metadata.json
│   │   ├── conus/
│   │   │   ├── historical/
│   │   │   └── ssp370/
│   │   ├── guam/
│   │   ├── hawaii/
│   │   └── puerto_rico/
│   ├── tas/
│   ├── tasmax/
│   └── tasmin/
│
├── reports/                       # Processing metadata
├── logs/                          # Processing logs
├── temp/                          # Temporary processing files
└── transformed/                   # Intermediate transformations
```

### Zarr Datasets

**Advantages of Zarr Format:**
- **Chunked storage**: Efficient partial reads
- **Compression**: ~10x smaller than raw NetCDF
- **Cloud-ready**: Works with S3, GCS, Azure
- **Parallel access**: Multiple processes can read simultaneously
- **Regional clipping**: Pre-clipped to US regions for faster analysis

**Compression:**
- Algorithm: ZSTD (level 5)
- Compression ratio: ~10:1 from raw NetCDF
- Trade-off: Slower writes, much faster reads and smaller storage

### County Statistics

**CSV Format:**
Each statistics file contains:
- `county_fips`: 5-digit FIPS code
- `county_name`: County name
- `state_name`: State name
- Climate-specific statistics:
  - **Precipitation**: annual_total, days_above_threshold, mean_daily, max_daily, dry_days
  - **Temperature**: annual_mean, annual_min, annual_max, freezing_days, hot_days

**Metadata JSON:**
Each CSV has accompanying metadata:
- Processing timestamp
- Variable and scenario
- Threshold values
- Statistics computed
- Data provenance

**Size**: ~300KB-2.5MB per region/scenario combination

## Regional County Shapefiles

### Location
`/Users/mihiarc/climate-zarr-slr/regional_counties/`

### Source
US Census Bureau TIGER/Line Shapefiles (2024)

### Files
Each region has five shapefile components:
- `.shp`: Geometry data
- `.shx`: Shape index
- `.dbf`: Attribute data (county names, FIPS codes, etc.)
- `.prj`: Projection information (NAD83)
- `.cpg`: Character encoding

### Regions

| Region | Counties | Size | Coverage |
|--------|----------|------|----------|
| `conus` | 3,108 | 123MB | Continental US (excluding Alaska, Hawaii, territories) |
| `alaska` | 30 | 1.1MB | Alaska |
| `hawaii` | 5 | 125KB | Hawaii |
| `guam` | 1 | 8KB | Guam |
| `puerto_rico` | 78 | 320KB | Puerto Rico and US Virgin Islands |

### Attributes
- `STATEFP`: 2-digit state FIPS code
- `COUNTYFP`: 3-digit county FIPS code
- `GEOID`: 5-digit combined FIPS code
- `NAME`: County name
- `NAMELSAD`: Full legal name
- `ALAND`: Land area (sq meters)
- `AWATER`: Water area (sq meters)

## Data Workflow

### 1. NetCDF → Zarr Conversion

Convert raw NetCDF files to compressed Zarr format with regional clipping:

```bash
# Interactive mode (recommended)
climate-zarr create-zarr

# Command-line mode
climate-zarr create-zarr data/raw_netcdf/pr/historical/ \
    -o climate_outputs/zarr/pr/conus/historical/pr_historical_conus.zarr \
    --region conus \
    --compression zstd \
    --compression-level 5
```

**What it does:**
- Concatenates yearly NetCDF files along time dimension
- Clips to specified region boundaries
- Applies compression (ZSTD level 5)
- Optimizes chunking for spatial queries
- Outputs hierarchical Zarr structure

### 2. County Statistics Calculation

Calculate county-level climate statistics from Zarr datasets:

```bash
# Interactive mode
climate-zarr county-stats

# Process single variable
climate-zarr county-stats \
    data/climate_outputs/zarr/pr/conus/historical/pr_historical_conus.zarr \
    conus -v pr -t 25.4

# Process all variables for a region
climate-zarr county-stats --region conus --variable all
```

**What it does:**
- Opens Zarr dataset
- Loads county shapefiles for region
- Rasterizes county polygons to data grid
- Calculates statistics per county
- Outputs CSV + metadata JSON

### 3. Data Integration

Combine climate statistics with demographic/economic data:

```python
import pandas as pd
import geopandas as gpd

# Load climate statistics
climate_df = pd.read_csv('climate_outputs/stats/pr/conus/historical/pr_stats.csv')

# Load county boundaries
counties_gdf = gpd.read_file('regional_counties/conus_counties.shp')

# Merge on FIPS code
merged = counties_gdf.merge(climate_df, left_on='GEOID', right_on='county_fips')

# Add population/income data (user-provided)
# merged = merged.merge(population_df, on='GEOID')
# merged = merged.merge(income_df, on='GEOID')

# Analyze relationships
# ... SLR research analysis ...
```

## Setting Up On a New Machine

### Prerequisites
1. External drive mounted (check `/Volumes/` on macOS)
2. Python 3.10+ installed
3. `uv` package manager installed

### Setup Steps

```bash
# 1. Clone repository
git clone https://github.com/mihiarc/climate-zarr-slr.git
cd climate-zarr-slr

# 2. Install dependencies
uv install

# 3. Verify external drive is mounted
ls /Volumes/SSD1TB/NorESM2-LM/

# 4. Create symbolic links (if not already present)
mkdir -p data
cd data
ln -s /Volumes/SSD1TB/NorESM2-LM raw_netcdf
ln -s /Volumes/SSD1TB/climate_outputs climate_outputs
ln -s /Volumes/SSD1TB/pr_historical_conus.zarr pr_historical_conus.zarr
ln -s /Volumes/SSD1TB/tas_historical_conus.zarr tas_historical_conus.zarr
ln -s /Volumes/SSD1TB/tasmax_historical_conus.zarr tasmax_historical_conus.zarr
ln -s /Volumes/SSD1TB/tasmin_historical_conus.zarr tasmin_historical_conus.zarr
cd ..

# 5. Verify setup
uv run python -c "import xarray as xr; print(xr.open_zarr('data/pr_historical_conus.zarr'))"

# 6. Run interactive wizard
uv run python src/climate_zarr/climate_cli.py wizard
```

### Troubleshooting

**External drive not mounted:**
```bash
# Check available volumes
ls /Volumes/

# If drive name differs, update symbolic links
ln -sf /Volumes/YOUR_DRIVE_NAME/NorESM2-LM data/raw_netcdf
```

**Broken symbolic links:**
```bash
# Check link status
ls -lah data/

# Re-create links
rm data/raw_netcdf  # if broken
ln -s /Volumes/SSD1TB/NorESM2-LM data/raw_netcdf
```

**Permission errors:**
```bash
# Ensure external drive is writable
chmod u+w /Volumes/SSD1TB/
```

## Data Access Examples

### Loading Zarr Datasets

```python
import xarray as xr

# Load full dataset
ds = xr.open_zarr('data/climate_outputs/zarr/pr/conus/historical/pr_historical_conus.zarr')

# Lazy loading - data only read when needed
print(ds)  # Shows structure without loading all data

# Select time range
ds_2000s = ds.sel(time=slice('2000', '2010'))

# Select spatial subset (lat/lon box)
ds_northeast = ds.sel(lat=slice(35, 45), lon=slice(-80, -70))

# Compute annual mean
annual_mean = ds.groupby('time.year').mean('time')

# Load data into memory (only when needed)
data = ds['pr'].values  # Triggers actual data read
```

### Reading County Statistics

```python
import pandas as pd

# Load CSV
stats_df = pd.read_csv('data/climate_outputs/stats/pr/conus/historical/pr_stats.csv')

# Load metadata
import json
with open('data/climate_outputs/stats/pr/conus/historical/pr_stats.metadata.json') as f:
    metadata = json.load(f)

# Filter to specific states
california = stats_df[stats_df['state_name'] == 'California']

# Sort by statistic
wettest_counties = stats_df.sort_values('annual_total_mm', ascending=False).head(10)
```

### Combining Climate Data with Shapefiles

```python
import geopandas as gpd

# Load county boundaries
counties = gpd.read_file('regional_counties/conus_counties.shp')

# Load climate statistics
climate = pd.read_csv('data/climate_outputs/stats/pr/conus/historical/pr_stats.csv')

# Merge
merged = counties.merge(climate, left_on='GEOID', right_on='county_fips')

# Plot
import matplotlib.pyplot as plt
merged.plot(column='annual_total_mm', legend=True, cmap='Blues')
plt.title('Annual Precipitation by County')
plt.show()
```

### Time Series Analysis

```python
import xarray as xr

# Load Zarr
ds = xr.open_zarr('data/climate_outputs/zarr/pr/conus/historical/pr_historical_conus.zarr')

# Get time series for specific location (lat/lon)
point_ts = ds.sel(lat=40.7, lon=-74.0, method='nearest')['pr']

# Convert to pandas for analysis
ts_df = point_ts.to_dataframe()

# Resample to monthly
monthly = ts_df.resample('ME').sum()

# Plot
monthly.plot()
```

### Comparing Scenarios

```python
import xarray as xr

# Load historical and future scenarios
hist = xr.open_zarr('data/climate_outputs/zarr/pr/conus/historical/pr_historical_conus.zarr')
future = xr.open_zarr('data/climate_outputs/zarr/pr/conus/ssp370/pr_ssp370_conus.zarr')

# Calculate baseline (historical mean)
hist_mean = hist['pr'].mean(dim='time')

# Calculate future mean
future_mean = future['pr'].mean(dim='time')

# Calculate change
change = future_mean - hist_mean

# Plot change
change.plot()
```

## Storage Considerations

### Disk Space Requirements

**Minimum (CONUS only, historical + 1 scenario):**
- Raw NetCDF: ~60GB (4 variables × 2 scenarios × 7.5GB)
- Processed Zarr: ~8GB (4 variables × 2 scenarios × 1GB)
- County Stats: ~50MB
- Shapefiles: ~125MB
- **Total: ~68GB**

**Recommended (All regions, all scenarios):**
- Raw NetCDF: ~300GB
- Processed Zarr: ~40GB
- County Stats: ~500MB
- Shapefiles: ~125MB
- **Total: ~340GB**

### Optimization Strategies

**1. Delete Raw NetCDF After Conversion**
Once Zarr files are created and validated, raw NetCDF can be archived or deleted:
```bash
# Archive to external storage
tar -czf NorESM2-LM_backup.tar.gz /Volumes/SSD1TB/NorESM2-LM/

# Delete raw files (after verifying Zarr works)
rm -rf /Volumes/SSD1TB/NorESM2-LM/
```
**Savings**: ~250GB

**2. Process Only Required Variables**
If you only need precipitation and temperature:
```bash
# Skip hurs and sfcWind processing
# Savings: ~40% of storage
```

**3. Regional Processing**
If research focuses on specific regions:
```bash
# Process only CONUS instead of all regions
# Savings: ~30% of processing time and storage
```

**4. Scenario Selection**
Choose relevant scenarios:
- Historical: Always needed for baseline
- SSP2-4.5: Medium emissions (most commonly cited)
- SSP5-8.5: High emissions (upper bound)
- Skip SSP1-2.6 and SSP3-7.0 unless needed

### Backup Strategy

**Critical files (store in git):**
- Code (`src/`, `tests/`)
- Documentation (`README.md`, `CLAUDE.md`, `DATA_ORGANIZATION.md`)
- Configuration (`pyproject.toml`, `.gitignore`)

**Data files (backup separately):**
- **Priority 1**: County statistics CSVs (~500MB)
  - Small enough for cloud backup (Dropbox, Google Drive)
  - Critical outputs for analysis

- **Priority 2**: Processed Zarr (~40GB)
  - Consider compressed archive
  - Faster to regenerate than raw data

- **Priority 3**: Raw NetCDF (~300GB)
  - Archive to external hard drive or cloud storage
  - Can be re-downloaded from CMIP6 archives if needed

**Suggested backup:**
```bash
# Backup county statistics (small, critical)
tar -czf county_stats_backup_$(date +%Y%m%d).tar.gz /Volumes/SSD1TB/climate_outputs/stats/

# Backup Zarr (medium, regeneratable)
tar -czf zarr_backup_$(date +%Y%m%d).tar.gz /Volumes/SSD1TB/climate_outputs/zarr/

# Archive raw NetCDF (large, re-downloadable)
# Store on separate external drive or cloud archive
```

## Best Practices

### Working with External Drive

**✅ DO:**
- Keep external drive mounted during analysis
- Use symbolic links for flexible file organization
- Set up proper read/write permissions
- Safely eject drive when not in use

**❌ DON'T:**
- Copy data files into git repository
- Modify raw NetCDF files (keep originals pristine)
- Hard-code absolute paths (use relative paths from project root)
- Forget to verify symbolic links after moving machines

### Data Management

**Version Control:**
```bash
# Only track code and documentation in git
git add src/ tests/ README.md pyproject.toml

# NEVER commit data files
# .gitignore already excludes: data/, *.zarr/, *.nc, *.csv, regional_counties/
```

**Data Versioning:**
```bash
# Version data separately using timestamped directories or metadata
climate_outputs/zarr/pr/conus/historical/pr_historical_conus_v20250110.zarr
```

**Processing Logs:**
```bash
# Keep logs for reproducibility
climate_outputs/logs/processing_20250110_143522.log
```

### Performance Optimization

**Chunking:**
- Zarr chunk size: 512x512 spatial, full time dimension
- Optimized for spatial queries (county aggregation)
- Adjust if working with time series analysis

**Parallel Processing:**
```bash
# Use multiple workers for county statistics
climate-zarr county-stats --workers 8

# Enable Dask distributed for large datasets
climate-zarr county-stats --distributed --workers 16
```

**Memory Management:**
```bash
# Process counties in chunks to avoid OOM
climate-zarr county-stats --chunk-counties --chunk-size 100
```

### Quality Assurance

**After Processing:**
1. **Verify Zarr integrity:**
```python
import zarr
z = zarr.open('data/climate_outputs/zarr/pr/conus/historical/pr_historical_conus.zarr')
print(z.tree())  # Check structure
print(z['pr'].shape)  # Verify dimensions
```

2. **Check statistics:**
```python
import pandas as pd
stats = pd.read_csv('data/climate_outputs/stats/pr/conus/historical/pr_stats.csv')
print(stats.describe())  # Check for anomalies
assert stats['county_fips'].notna().all()  # Verify no missing FIPS
```

3. **Validate spatial joins:**
```python
import geopandas as gpd
counties = gpd.read_file('regional_counties/conus_counties.shp')
assert len(stats) == len(counties)  # All counties processed
```

## Data Provenance

### Source
**CMIP6 NorESM2-LM Model**
- Institution: Norwegian Climate Centre (NCC)
- Model: Norwegian Earth System Model - Low Resolution
- Experiment: CMIP6 (Coupled Model Intercomparison Project Phase 6)
- Variant: r1i1p1f1 (realization 1, initialization 1, physics 1, forcing 1)
- Grid: Global native grid (gn)

### Citation
```
Seland, Ø., Bentsen, M., Olivié, D., et al. (2020). NorESM2-LM model output
prepared for CMIP6. Earth System Grid Federation.
https://doi.org/10.22033/ESGF/CMIP6.8217
```

### Download Source
Data downloaded from ESGF (Earth System Grid Federation):
- Portal: https://esgf-node.llnl.gov/
- Search: CMIP6 → NorESM2-LM → daily → pr/tas/tasmax/tasmin
- Variables: pr, tas, tasmax, tasmin, hurs, sfcWind
- Scenarios: historical, ssp126, ssp245, ssp370, ssp585

### Processing History
All processed datasets include metadata tracking:
- Original source files
- Processing date and software versions
- Transformations applied (clipping, compression)
- Quality control checks performed

### License
CMIP6 data is freely available under the Creative Commons Attribution 4.0
International License (CC BY 4.0).

## Appendix: Data Dictionary

### Climate Variables (NetCDF/Zarr)

| Field | Type | Description | Units | Range |
|-------|------|-------------|-------|-------|
| `pr` | float32 | Precipitation (converted from kg m⁻² s⁻¹) | mm/day | 0-1000 |
| `tas` | float32 | Near-surface air temperature (converted from K) | °C | -60 to 50 |
| `tasmax` | float32 | Daily maximum temperature (converted from K) | °C | -50 to 60 |
| `tasmin` | float32 | Daily minimum temperature (converted from K) | °C | -70 to 40 |
| `time` | datetime64 | Date | YYYY-MM-DD | 1950-2100 |
| `lat` | float32 | Latitude | degrees_north | -90 to 90 |
| `lon` | float32 | Longitude | degrees_east | -180 to 180 |

### County Statistics (CSV)

**Precipitation Statistics:**
| Field | Type | Description | Units |
|-------|------|-------------|-------|
| `county_fips` | string | 5-digit FIPS code | - |
| `county_name` | string | County name | - |
| `state_name` | string | State name | - |
| `annual_total_mm` | float | Total annual precipitation | mm |
| `days_above_threshold` | int | Days exceeding threshold (default 25.4mm) | days |
| `mean_daily_mm` | float | Mean daily precipitation | mm |
| `max_daily_mm` | float | Maximum daily precipitation | mm |
| `dry_days` | int | Days with <1mm precipitation | days |

**Temperature Statistics:**
| Field | Type | Description | Units |
|-------|------|-------------|-------|
| `county_fips` | string | 5-digit FIPS code | - |
| `county_name` | string | County name | - |
| `state_name` | string | State name | - |
| `annual_mean_c` | float | Mean annual temperature | °C |
| `annual_min_c` | float | Minimum temperature | °C |
| `annual_max_c` | float | Maximum temperature | °C |
| `annual_range_c` | float | Temperature range (max - min) | °C |
| `freezing_days` | int | Days below 0°C | days |
| `hot_days` | int | Days above threshold (default 32°C) | days |

### Shapefile Attributes

| Field | Type | Description |
|-------|------|-------------|
| `STATEFP` | string | 2-digit state FIPS code |
| `COUNTYFP` | string | 3-digit county FIPS code |
| `GEOID` | string | 5-digit combined FIPS (STATEFP + COUNTYFP) |
| `NAME` | string | County name |
| `NAMELSAD` | string | Legal/statistical area description |
| `ALAND` | int64 | Land area (square meters) |
| `AWATER` | int64 | Water area (square meters) |
| `INTPTLAT` | float | Internal point latitude |
| `INTPTLON` | float | Internal point longitude |

---

**Last Updated**: 2025-10-10
**Maintainer**: Chris Mihiar (chris.mihiar.fs@gmail.com)
**Project**: https://github.com/mihiarc/climate-zarr-slr
