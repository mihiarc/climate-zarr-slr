# Data Inventory - NorESM2-LM Climate Projections

**Date**: 2025-10-11
**Source**: NASA NEX-GDDP-CMIP6 (Downscaled CMIP6 Climate Projections)

## Summary

✅ **Status**: Complete collection - All scenarios and variables present

- **Total Files**: 1,377 NetCDF files (~310 GB)
- **Model**: NorESM2-LM (Norwegian Earth System Model)
- **Variables**: 4 (precipitation, temperature, max temp, min temp)
- **Scenarios**: 4 (SSP1-2.6, SSP2-4.5, SSP3-7.0, SSP5-8.5)
- **Time Coverage**: 2015-2100 (86 years)
- **Spatial Resolution**: 0.25° × 0.25° (25km at equator)
- **Format**: NetCDF4 with CF-1.7 metadata conventions

## Detailed Inventory

### Variables

| Variable | Description | Unit | Files | Storage |
|----------|-------------|------|-------|---------|
| `pr` | Daily precipitation | mm/day | 344 | ~73 GB |
| `tas` | Daily mean temperature | °C | 345* | ~78 GB |
| `tasmax` | Daily maximum temperature | °C | 344 | ~80 GB |
| `tasmin` | Daily minimum temperature | °C | 344 | ~80 GB |

*Note: tas/ssp245 has 87 files due to backed-up corrupted file (`2074.nc.corrupted`)

### Climate Scenarios

| Scenario | Name | Description | Variables | Complete |
|----------|------|-------------|-----------|----------|
| **ssp126** | Low emissions | Sustainability pathway limiting warming to ~1.8°C | 4/4 | ✅ |
| **ssp245** | Medium emissions | Middle-of-the-road pathway with ~2.7°C warming | 4/4 | ✅ |
| **ssp370** | High emissions | Regional rivalry with ~3.6°C warming | 4/4 | ✅ |
| **ssp585** | Very high emissions | Fossil-fueled development with ~4.4°C warming | 4/4 | ✅ |

### Completeness by Variable and Scenario

|  | ssp126 | ssp245 | ssp370 | ssp585 |
|--|--------|--------|--------|--------|
| **pr** | 86/86 ✅ | 86/86 ✅ | 86/86 ✅ | 86/86 ✅ |
| **tas** | 86/86 ✅ | 86/86 ✅ | 86/86 ✅ | 86/86 ✅ |
| **tasmax** | 86/86 ✅ | 86/86 ✅ | 86/86 ✅ | 86/86 ✅ |
| **tasmin** | 86/86 ✅ | 86/86 ✅ | 86/86 ✅ | 86/86 ✅ |

## Data Source Details

### NASA NEX-GDDP-CMIP6

The NASA Earth Exchange Global Daily Downscaled Projections (NEX-GDDP-CMIP6) dataset is derived from the CMIP6 Global Climate Model (GCM) runs using the Bias-Correction Spatial Disaggregation (BCSD) method.

**Key Features**:
- **Downscaling**: Native CMIP6 (~100km) → NEX-GDDP-CMIP6 (25km)
- **Bias Correction**: Corrected against historical observations (GMET)
- **Global Coverage**: Complete global land areas (excluding Antarctica)
- **Daily Temporal Resolution**: 365/366 days per year
- **Institution**: NASA Ames Research Center, NASA Earth Exchange

### AWS S3 Access

**Bucket**: `s3://nex-gddp-cmip6` (us-west-2)
**Public Access**: Yes (no AWS credentials required)
**License**: Creative Commons Zero (CC0) as of September 2022

**Download URL Pattern**:
```
https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/
  NorESM2-LM/{scenario}/r1i1p1f1/{variable}/
    {variable}_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}.nc
```

**Example**:
```bash
# Download single year
curl -O "https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/NorESM2-LM/ssp245/r1i1p1f1/tas/tas_day_NorESM2-LM_ssp245_r1i1p1f1_gn_2050.nc"

# List all files for a variable/scenario
aws s3 ls s3://nex-gddp-cmip6/NEX-GDDP-CMIP6/NorESM2-LM/ssp245/r1i1p1f1/tas/ --no-sign-request
```

## Data Quality Notes

### File Corruption Issue (Resolved)

**Date**: 2025-10-11
**File**: `tas/ssp245/tas_day_NorESM2-LM_ssp245_r1i1p1f1_gn_2074.nc`
**Issue**: HDF5 corruption (Error -101: NetCDF: HDF error)
**Resolution**: Re-downloaded from AWS S3 (233 MB)
**Backup**: Original corrupted file saved as `*.corrupted`

### Data Validation

All files should meet these criteria:
- ✅ Readable with xarray/netCDF4
- ✅ Contains expected variables and dimensions
- ✅ Time dimension matches filename year
- ✅ 365 days per year (366 for leap years)
- ✅ Spatial dimensions: lat=600, lon=1440

**Validation Command**:
```bash
uv run python -c "
import xarray as xr
ds = xr.open_dataset('path/to/file.nc')
print(f'Variable: {list(ds.data_vars)}')
print(f'Shape: {ds[var].shape}')
print(f'Time range: {ds.time.values[0]} to {ds.time.values[-1]}')
"
```

## Storage Organization

```
/Volumes/SSD1TB/NorESM2-LM/
├── pr/
│   ├── ssp126/    # 86 files, 19 GB
│   ├── ssp245/    # 86 files, 18 GB
│   ├── ssp370/    # 86 files, 18 GB (PROCESSED)
│   └── ssp585/    # 86 files, 18 GB
├── tas/
│   ├── ssp126/    # 86 files, 19 GB
│   ├── ssp245/    # 86 files, 20 GB (IN PROGRESS)
│   ├── ssp370/    # 86 files, 19 GB (PROCESSED)
│   └── ssp585/    # 86 files, 20 GB
├── tasmax/
│   ├── ssp126/    # 86 files, 20 GB
│   ├── ssp245/    # 86 files, 20 GB
│   ├── ssp370/    # 86 files, 20 GB (PROCESSED)
│   └── ssp585/    # 86 files, 20 GB
└── tasmin/
    ├── ssp126/    # 86 files, 20 GB
    ├── ssp245/    # 86 files, 20 GB
    ├── ssp370/    # 86 files, 20 GB (PROCESSED)
    └── ssp585/    # 86 files, 20 GB
```

## Processing Status

### Stage 1: NetCDF → Zarr Conversion

| Scenario | Status | Zarr Output Location |
|----------|--------|---------------------|
| ssp370 | ✅ Complete | `/Volumes/SSD1TB/climate_outputs/zarr/*/ssp370/` |
| ssp245 | 🔄 In Progress | `/Volumes/SSD1TB/climate_outputs/zarr/*/ssp245/` |
| ssp126 | ⏳ Pending | - |
| ssp585 | ⏳ Pending | - |

### Stage 2: County Statistics

| Scenario | Status | Output Location |
|----------|--------|-----------------|
| ssp370 | ✅ Complete | `/Volumes/SSD1TB/climate_metrics_ssp370.csv` |
| ssp245 | ⏳ Pending | - |
| ssp126 | ⏳ Pending | - |
| ssp585 | ⏳ Pending | - |

## Additional Available Variables on AWS

NEX-GDDP-CMIP6 provides additional climate variables beyond those currently downloaded:

- **hurs**: Near-surface relative humidity (%)
- **huss**: Near-surface specific humidity (kg/kg)
- **rlds**: Surface downwelling longwave radiation (W/m²)
- **rsds**: Surface downwelling shortwave radiation (W/m²)
- **sfcWind**: Near-surface wind speed (m/s)

These can be downloaded using the same URL pattern if needed for future analysis.

## Citation

When using this data, cite:

> Thrasher, B., Wang, W., Michaelis, A., Melton, F., Lee, T., & Nemani, R. (2022). NASA Global Daily Downscaled Projections, CMIP6. Scientific Data, 9(1), 262. https://doi.org/10.1038/s41597-022-01393-4

## Maintenance Log

| Date | Action | Details |
|------|--------|---------|
| 2025-10-11 | File repair | Re-downloaded tas/ssp245/2074.nc from AWS S3 |
| 2025-10-11 | Inventory | Created comprehensive data inventory documentation |
| 2025-10-11 | Processing | Started Stage 1 processing for ssp245 scenario |

---

**Last Updated**: 2025-10-11
**Maintained By**: climate-zarr-slr project
