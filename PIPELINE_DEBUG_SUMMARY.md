# Pipeline Debugging & Fixes - 2025-10-11

## Issues Identified

1. **Interactive prompts blocking automation** - Scripts used `yes |` piping which failed
2. **CLI defaulted to interactive mode** - Caused user confirmation prompts
3. **Missing log directories** - Logs not created in consistent location
4. **Technical debt** - Multiple script versions causing confusion

## Changes Made

### 1. CLI Made Non-Interactive by Default (`src/climate_zarr/climate_cli.py`)

**Changed**: `interactive` parameter default from `True` → `False` (lines 262, 403)

```python
# Before
interactive: Annotated[bool, ...] = True

# After  
interactive: Annotated[bool, ...] = False
```

**Impact**: CLI now runs without prompts by default. Use `--interactive` flag to enable prompts.

### 2. Unified Processing Script (`scripts/process_scenario_stage1.sh`)

**Removed**: Old interactive version with `yes |` workaround
**Created**: Single non-interactive script with:
- Smart skip logic (checks for `.zarray` files)
- Timestamped logs in `logs/` directory
- Progress tracking (completed vs failed)
- Better error messages

**Key features**:
```bash
# Checks if already completed
if [ -f "$OUTPUT_ZARR/.zarray" ]; then
    echo "✅ Already exists, skipping"
    continue
fi

# Logs everything
LOG_FILE="logs/stage1_${SCENARIO}_$(date +%Y%m%d_%H%M%S).log"
```

### 3. Real-Time Monitoring Dashboard (`scripts/monitor_pipeline.py`)

**Features**:
- Live progress tracking for all 5 scenarios
- Zarr completion status (340+ datasets)
- Statistics generation status (44+ CSV files)
- Storage usage monitoring (32GB+)
- Auto-refresh every 5 seconds
- Color-coded status indicators

**Usage**:
```bash
uv run python scripts/monitor_pipeline.py
```

## Current Pipeline Status

### Completed Datasets
- **Historical**: 20/20 Zarr stores ✅
- **SSP370**: 20/20 Zarr stores ✅  
- **SSP126**: Processing (1/20 in progress)
- **SSP245**: Queued
- **SSP585**: Queued

### Statistics Generated
- 44 CSV files created
- Variables: pr, tas, tasmax, tasmin
- Regions: CONUS, Alaska, Hawaii, Puerto Rico, Guam
- Scenarios: historical, ssp370 (partial)

### Storage
- **Zarr stores**: 32GB
- **Location**: `/Volumes/SSD1TB/climate_outputs/`
- **Organization**: `zarr/{variable}/{region}/{scenario}/`

## How to Use

### Start Processing a Scenario
```bash
# Single scenario
./scripts/process_scenario_stage1.sh ssp126 &

# Multiple scenarios in parallel
./scripts/process_scenario_stage1.sh ssp245 > logs/ssp245.log 2>&1 &
./scripts/process_scenario_stage1.sh ssp585 > logs/ssp585.log 2>&1 &
```

### Monitor Progress
```bash
# Real-time dashboard (recommended)
uv run python scripts/monitor_pipeline.py

# Check logs
tail -f logs/stage1_ssp126_*.log

# Quick status check
find /Volumes/SSD1TB/climate_outputs/zarr -name ".zarray" | wc -l
```

### Manual CLI Usage
```bash
# Non-interactive (default)
uv run climate-zarr create-zarr \
    /Volumes/SSD1TB/NorESM2-LM/pr/ssp245/ \
    -o output.zarr \
    --region conus \
    --compression zstd

# Interactive (opt-in)
uv run climate-zarr create-zarr --interactive
```

## Technical Debt Removed

1. ❌ Removed `process_scenario_stage1.sh` (old interactive version)
2. ✅ Single script: `process_scenario_stage1.sh` (non-interactive)
3. ✅ CLI defaults to non-interactive
4. ✅ Consistent logging structure
5. ✅ No more `yes |` workarounds

## Next Steps

1. **Complete SSP126, SSP245, SSP585** - Let scripts finish (est. 2-4 hours each)
2. **Stage 2: Generate Statistics** - Run `process_scenario_stage2.sh` for each scenario
3. **Verify Data Quality** - Use monitoring dashboard to check completion
4. **Generate Reports** - Create summary statistics and visualizations

## Files Modified

- `src/climate_zarr/climate_cli.py` - Changed interactive default
- `scripts/process_scenario_stage1.sh` - Unified non-interactive script
- `scripts/monitor_pipeline.py` - New monitoring dashboard

## Files Removed

- `scripts/process_scenario_stage1_noninteractive.sh` - Merged into main script

