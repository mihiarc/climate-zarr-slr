# Utility Scripts

This directory contains utility scripts for processing, monitoring, and visualizing climate data in the climate-zarr-slr project.

## Processing Scripts

### `batch_process_county_stats.py`
Batch processing script for calculating county-level climate statistics across multiple regions and scenarios.

**Usage:**
```bash
uv run python scripts/batch_process_county_stats.py
```

**Features:**
- Process multiple regions (conus, alaska, hawaii, etc.) in sequence
- Handle multiple climate variables (pr, tas, tasmax, tasmin)
- Process multiple scenarios (historical, ssp126, ssp245, ssp370, ssp585)
- Parallel processing with configurable worker count
- Progress tracking and error reporting

### `transform_climate_stats.py`
Transform and reformat climate statistics outputs for different use cases.

**Usage:**
```bash
uv run python scripts/transform_climate_stats.py <input_csv> <output_format>
```

**Supported formats:**
- Long format (one row per county-variable-scenario)
- Wide format (one row per county, columns for each variable)
- GeoJSON (spatial format with statistics as properties)

### `monitor_processing.py`
Real-time monitoring script for long-running climate data processing jobs.

**Usage:**
```bash
uv run python scripts/monitor_processing.py
```

**Features:**
- Monitor CPU and memory usage
- Track processing progress
- Estimate time remaining
- Alert on errors or stalls

## Visualization Scripts

### `create_precipitation_map.py`
Generate precipitation maps from county-level statistics.

**Usage:**
```bash
uv run python scripts/create_precipitation_map.py \
    --input data/climate_outputs/stats/pr/conus/historical/pr_stats.csv \
    --output precipitation_map.png \
    --variable annual_total_mm
```

**Features:**
- Multiple color schemes
- Customizable variable selection
- County boundary overlay
- Legend and title customization

### `create_temperature_map.py`
Generate temperature maps from county-level statistics.

**Usage:**
```bash
uv run python scripts/create_temperature_map.py \
    --input data/climate_outputs/stats/tas/conus/historical/tas_stats.csv \
    --output temperature_map.png \
    --variable annual_mean_c
```

**Features:**
- Temperature-appropriate color schemes
- Multiple temperature statistics (mean, min, max, range)
- County boundary overlay

### `visualize_missing_counties.py`
Identify and visualize counties with missing climate data.

**Usage:**
```bash
uv run python scripts/visualize_missing_counties.py \
    --stats data/climate_outputs/stats/pr/conus/historical/pr_stats.csv \
    --counties regional_counties/conus_counties.shp \
    --output missing_counties.png
```

**Features:**
- Highlight counties with incomplete data
- Generate summary statistics on missing data
- Export list of missing county FIPS codes
- Visual comparison with complete county shapefile

## Common Patterns

### Running Multiple Regions
```bash
# Process all regions for precipitation
for region in conus alaska hawaii guam puerto_rico; do
    uv run python scripts/batch_process_county_stats.py \
        --region $region \
        --variable pr
done
```

### Creating Comparison Maps
```bash
# Compare historical and future scenarios
uv run python scripts/create_precipitation_map.py \
    --input data/climate_outputs/stats/pr/conus/historical/pr_stats.csv \
    --output pr_historical.png

uv run python scripts/create_precipitation_map.py \
    --input data/climate_outputs/stats/pr/conus/ssp370/pr_stats.csv \
    --output pr_ssp370.png
```

### Monitoring Long-Running Jobs
```bash
# Start processing in background
uv run python scripts/batch_process_county_stats.py > processing.log 2>&1 &

# Monitor in separate terminal
uv run python scripts/monitor_processing.py
```

## Dependencies

All scripts use the main project dependencies from `pyproject.toml`. Key requirements:
- xarray, zarr - For climate data access
- pandas, geopandas - For statistics and spatial data
- matplotlib, cartopy - For visualization
- rich - For terminal output and progress bars

## Development

When adding new scripts:
1. Include docstrings with usage examples
2. Use argparse or typer for CLI arguments
3. Add rich progress bars for long operations
4. Include error handling and logging
5. Update this README with script description

## Integration with Main CLI

These scripts complement the main CLI (`climate-zarr`) but provide more specialized functionality:

```bash
# Main CLI (interactive, guided)
climate-zarr wizard

# Utility scripts (batch, specialized)
uv run python scripts/batch_process_county_stats.py
```

Use the main CLI for:
- Interactive data exploration
- Single region/variable processing
- Learning the workflow

Use utility scripts for:
- Batch processing multiple regions
- Custom visualizations
- Monitoring and debugging
- Data transformation

## See Also

- [Main Documentation](../README.md)
- [Data Organization](../DATA_ORGANIZATION.md)
- [Examples](../examples/)
