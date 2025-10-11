# Additional Variables - Current Status

**Last Updated**: 2025-10-11

## Current Holdings

### ✅ Wind Speed (sfcWind)

| Scenario | Status | Files | Storage | Years | Notes |
|----------|--------|-------|---------|-------|-------|
| ssp126 | ✅ Complete | 86/86 | 23.9 GB | 2015-2100 | Version 2.0 files |
| ssp245 | ❌ Missing | 0/86 | - | - | **Need to download** |
| ssp370 | ❌ Missing | 0/86 | - | - | **Need to download** |
| ssp585 | ❌ Missing | 0/86 | - | - | **Need to download** |

**Total**: 86/344 files (25% complete), 23.9/96 GB

### ✅ Relative Humidity (hurs)

| Scenario | Status | Files | Storage | Years | Notes |
|----------|--------|-------|---------|-------|-------|
| ssp126 | ❌ Missing | 0/86 | - | - | **Need to download** |
| ssp245 | ✅ Complete | 86/86 | 21.9 GB | 2015-2100 | Ready |
| ssp370 | ✅ Complete | 86/86 | 21.9 GB | 2015-2100 | Ready |
| ssp585 | ✅ Complete | 86/86 | 22.0 GB | 2015-2100 | Ready |

**Total**: 258/344 files (75% complete), 65.8/88 GB

## Missing Data to Download

### Priority 1: Complete Wind Speed (sfcWind)

**Scenarios Needed**: ssp245, ssp370, ssp585
**Total**: 258 files, ~72 GB
**Estimated Download Time**: 4-6 hours

**Why Critical**:
- Essential for storm surge modeling
- Direct application to coastal flood risk
- Combine with precipitation for compound flooding analysis
- Currently only have low-emissions scenario (ssp126)

**Download Command**:
```bash
# Download sfcWind for ssp245
./scripts/download_additional_variable.sh sfcWind ssp245
```

### Priority 2: Complete Relative Humidity (hurs)

**Scenarios Needed**: ssp126
**Total**: 86 files, ~22 GB
**Estimated Download Time**: 1-2 hours

**Why Useful**:
- Complete the humidity dataset for all scenarios
- Heat index calculations across all future pathways
- Relatively small download

**Download Command**:
```bash
# Download hurs for ssp126
./scripts/download_additional_variable.sh hurs ssp126
```

### Priority 3: Add Solar Radiation (rsds)

**Scenarios Needed**: All 4 (ssp126, ssp245, ssp370, ssp585)
**Total**: 344 files, ~88 GB
**Estimated Download Time**: 5-7 hours

**Why Valuable**:
- Water stress and drought vulnerability
- Agricultural productivity analysis
- Solar energy potential for resilience planning

## Storage Summary

**Current Disk Usage**:
- Used: 516 GB
- Available: 415 GB
- Total: 931 GB

**After Completing Missing Downloads**:

| Item | Files | Storage | Available After |
|------|-------|---------|-----------------|
| Current | - | 516 GB | 415 GB |
| + sfcWind (3 scenarios) | 258 | +72 GB | 343 GB ✅ |
| + hurs (1 scenario) | 86 | +22 GB | 321 GB ✅ |
| + rsds (4 scenarios) | 344 | +88 GB | 233 GB ✅ |
| **Total** | +688 | +182 GB | 233 GB ✅ |

**Conclusion**: Plenty of space for all recommended downloads

## Recommended Download Order

### Phase 1 (Immediate) - Complete Wind Speed
```bash
# Download missing wind speed scenarios
./scripts/download_additional_variable.sh sfcWind ssp245  # ~24 GB, critical
./scripts/download_additional_variable.sh sfcWind ssp370  # ~24 GB
./scripts/download_additional_variable.sh sfcWind ssp585  # ~24 GB
```

**Rationale**: Most critical for SLR research, complete existing dataset

### Phase 2 (Next) - Complete Humidity
```bash
# Download missing humidity scenario
./scripts/download_additional_variable.sh hurs ssp126  # ~22 GB
```

**Rationale**: Quick download, completes existing dataset

### Phase 3 (Optional) - Add Solar Radiation
```bash
# Download all solar radiation scenarios
for scenario in ssp126 ssp245 ssp370 ssp585; do
  ./scripts/download_additional_variable.sh rsds $scenario
done
```

**Rationale**: New variable, useful for water stress analysis

## Integration into Processing Pipeline

### After Downloads Complete

**1. Update Stage 1 Script**

Modify `scripts/process_scenario_stage1.sh` to process additional variables:

```bash
# Current variables
for VAR in pr tas tasmax tasmin; do
  # ... processing ...
done

# Add new variables
for VAR in sfcWind hurs rsds; do
  # ... processing ...
done
```

**2. Create New Statistics Processors**

- `src/climate_zarr/processors/wind_processor.py`
- `src/climate_zarr/processors/humidity_processor.py`
- `src/climate_zarr/processors/radiation_processor.py`

**3. Update County Statistics**

Add new columns to final CSV output:
- Wind: `wind_mean`, `wind_95th`, `days_high_wind`
- Humidity: `humidity_mean`, `days_high_humidity`
- Solar: `annual_solar_radiation`, `solar_trend`

## Research Applications

### With Complete Wind Speed Data

**Compound Coastal Risk Analysis**:
```python
# Identify counties with multiple concurrent risks
high_risk = counties.where(
    (wind_extremes > threshold) &
    (precip_intensity > threshold) &
    (slr_exposure > threshold)
)
```

**Storm Surge Vulnerability**:
```python
# Calculate wind-driven flood risk
surge_risk = wind_95th * fetch_distance * storm_frequency
```

### With Complete Humidity Data

**Heat Stress Assessment**:
```python
# Calculate dangerous heat-humidity days
heat_index = calculate_heat_index(temperature, humidity)
dangerous_days = (heat_index > 105).sum(dim='time')
```

### With Solar Radiation Data

**Agricultural Drought Risk**:
```python
# Potential evapotranspiration
pet = hargreaves_samani(temp_mean, temp_range, solar_radiation)
aridity_index = precipitation / pet
drought_risk = aridity_index < 0.65
```

## Notes

- **Wind Speed Files**: Version 2.0 format (includes `_v2.0` suffix)
- **Compatibility**: All variables use same spatial grid (0.25° × 0.25°)
- **Processing**: Can reuse existing pipeline with minor modifications
- **Output**: Same hierarchical Zarr structure as current variables

---

**Next Action**: Run download scripts for missing sfcWind scenarios (Phase 1)
