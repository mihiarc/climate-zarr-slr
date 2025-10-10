# Scenario Processing Guide

## Overview

This guide explains how to process additional climate scenarios after completing SSP3-7.0. The processing pipeline consists of two main stages:

1. **Stage 1: NetCDF → Zarr Conversion** - Convert raw NetCDF files to optimized Zarr format
2. **Stage 2: County Statistics Calculation** - Extract county-level statistics from Zarr files

## Current Status

### Completed Processing

**Scenario**: SSP3-7.0 (ssp370)
- ✅ **Stage 1**: NetCDF → Zarr conversion complete
- ✅ **Stage 2**: County statistics complete
- ✅ **Final Output**: `climate_metrics_ssp370.csv` (25MB, 277,781 rows)

**Variables Processed**: 4
- `pr` - Precipitation
- `tas` - Air Temperature
- `tasmax` - Daily Maximum Temperature
- `tasmin` - Daily Minimum Temperature

**Regions Processed**: 5
- CONUS (3,109 counties)
- Alaska (29 counties)
- Hawaii (5 counties)
- Puerto Rico (78 counties)
- Guam (1 county)

### Available for Processing

**Raw NetCDF Data**:
```
/Volumes/SSD1TB/NorESM2-LM/
├── pr/
│   ├── historical/     ✅ (processed)
│   ├── ssp126/         ⏳ (available)
│   ├── ssp245/         ⏳ (available)
│   ├── ssp370/         ✅ (processed)
│   └── ssp585/         ⏳ (available)
├── tas/
│   └── [same structure]
├── tasmax/
│   └── [same structure]
└── tasmin/
    └── [same structure]
```

**Scenarios Available**:
- ✅ `historical` - 1950-2014 (baseline)
- ⏳ `ssp126` - Low emissions (not yet processed)
- ⏳ `ssp245` - Medium emissions (not yet processed)
- ✅ `ssp370` - Medium-high emissions (completed)
- ⏳ `ssp585` - High emissions (not yet processed)

## Processing Workflow for New Scenarios

### Recommended Processing Order

For SLR research, process scenarios in this order:

1. **SSP2-4.5** (`ssp245`) - Medium emissions, most commonly cited
2. **SSP5-8.5** (`ssp585`) - High emissions, upper bound
3. **SSP1-2.6** (`ssp126`) - Low emissions, mitigation scenario (optional)

### Stage 1: NetCDF → Zarr Conversion

Convert raw NetCDF files to compressed Zarr format for efficient analysis.

#### Step 1.1: Process Single Variable-Region-Scenario

```bash
# Example: Convert SSP2-4.5 precipitation data for CONUS
climate-zarr create-zarr \
    /Volumes/SSD1TB/NorESM2-LM/pr/ssp245/ \
    -o /Volumes/SSD1TB/climate_outputs/zarr/pr/conus/ssp245/conus_ssp245_pr_daily.zarr \
    --region conus \
    --compression zstd \
    --compression-level 5 \
    --concat-dim time \
    --interactive false
```

**Key Parameters**:
- `--region` - Clips data to specific region (conus, alaska, hawaii, puerto_rico, guam)
- `--compression zstd` - Best compression ratio for climate data
- `--compression-level 5` - Balance between compression and speed
- `--concat-dim time` - Concatenate yearly files along time dimension
- `--interactive false` - Non-interactive mode for scripting

#### Step 1.2: Batch Process All Variables for One Region

```bash
#!/bin/bash
# Process all 4 variables for CONUS, SSP2-4.5

SCENARIO="ssp245"
REGION="conus"

for VAR in pr tas tasmax tasmin; do
    echo "Processing $VAR for $REGION ($SCENARIO)..."

    climate-zarr create-zarr \
        /Volumes/SSD1TB/NorESM2-LM/$VAR/$SCENARIO/ \
        -o /Volumes/SSD1TB/climate_outputs/zarr/$VAR/$REGION/$SCENARIO/${REGION}_${SCENARIO}_${VAR}_daily.zarr \
        --region $REGION \
        --compression zstd \
        --compression-level 5 \
        --concat-dim time \
        --interactive false

    if [ $? -eq 0 ]; then
        echo "✅ $VAR completed successfully"
    else
        echo "❌ $VAR failed"
        exit 1
    fi
done

echo "✅ All variables processed for $REGION ($SCENARIO)"
```

#### Step 1.3: Batch Process All Regions for One Scenario

```bash
#!/bin/bash
# Process all regions for SSP2-4.5, single variable

SCENARIO="ssp245"
VAR="pr"

for REGION in conus alaska hawaii puerto_rico guam; do
    echo "Processing $VAR for $REGION ($SCENARIO)..."

    climate-zarr create-zarr \
        /Volumes/SSD1TB/NorESM2-LM/$VAR/$SCENARIO/ \
        -o /Volumes/SSD1TB/climate_outputs/zarr/$VAR/$REGION/$SCENARIO/${REGION}_${SCENARIO}_${VAR}_daily.zarr \
        --region $REGION \
        --compression zstd \
        --compression-level 5 \
        --concat-dim time \
        --interactive false

    if [ $? -eq 0 ]; then
        echo "✅ $REGION completed"
    else
        echo "❌ $REGION failed"
        exit 1
    fi
done

echo "✅ All regions processed for $VAR ($SCENARIO)"
```

#### Step 1.4: Complete Scenario Processing (All Variables, All Regions)

Save this as `scripts/process_scenario_stage1.sh`:

```bash
#!/bin/bash
# Complete Stage 1: NetCDF → Zarr conversion for entire scenario
# Usage: ./process_scenario_stage1.sh ssp245

set -e  # Exit on error

SCENARIO=$1

if [ -z "$SCENARIO" ]; then
    echo "Usage: $0 <scenario>"
    echo "Available scenarios: ssp126, ssp245, ssp585"
    exit 1
fi

echo "========================================="
echo "Stage 1: NetCDF → Zarr Conversion"
echo "Scenario: $SCENARIO"
echo "========================================="

TOTAL_DATASETS=20  # 4 variables × 5 regions
COMPLETED=0

for VAR in pr tas tasmax tasmin; do
    for REGION in conus alaska hawaii puerto_rico guam; do
        echo ""
        echo "[$((COMPLETED+1))/$TOTAL_DATASETS] Processing $REGION - $VAR ($SCENARIO)..."

        INPUT_DIR="/Volumes/SSD1TB/NorESM2-LM/$VAR/$SCENARIO/"
        OUTPUT_ZARR="/Volumes/SSD1TB/climate_outputs/zarr/$VAR/$REGION/$SCENARIO/${REGION}_${SCENARIO}_${VAR}_daily.zarr"

        # Create output directory
        mkdir -p "$(dirname "$OUTPUT_ZARR")"

        # Convert NetCDF to Zarr
        uv run climate-zarr create-zarr \
            "$INPUT_DIR" \
            -o "$OUTPUT_ZARR" \
            --region "$REGION" \
            --compression zstd \
            --compression-level 5 \
            --concat-dim time \
            --interactive false

        if [ $? -eq 0 ]; then
            COMPLETED=$((COMPLETED+1))
            echo "✅ [$COMPLETED/$TOTAL_DATASETS] $REGION-$VAR completed"
        else
            echo "❌ $REGION-$VAR failed"
            exit 1
        fi

        # Brief pause to avoid overwhelming the system
        sleep 2
    done
done

echo ""
echo "========================================="
echo "✅ Stage 1 Complete for $SCENARIO"
echo "Processed: $COMPLETED/$TOTAL_DATASETS datasets"
echo "========================================="
echo ""
echo "Next step: Run Stage 2 (County Statistics)"
echo "  ./process_scenario_stage2.sh $SCENARIO"
```

### Stage 2: County Statistics Calculation

Extract county-level statistics from Zarr files.

#### Step 2.1: Update Batch Processing Script

The current `scripts/batch_process_county_stats.py` is hardcoded to process `ssp370` (line 64).

**Option A: Modify for New Scenario**

Edit `scripts/batch_process_county_stats.py`:

```python
# Line 64: Change scenario
self.scenario = "ssp245"  # Change from "ssp370" to your target scenario
```

Then run:
```bash
uv run python scripts/batch_process_county_stats.py --workers 8
```

**Option B: Create Scenario-Specific Script**

Create `scripts/batch_process_county_stats_scenario.py`:

```python
#!/usr/bin/env python
"""
Batch process county statistics for a specific scenario.
Usage: python batch_process_county_stats_scenario.py --scenario ssp245
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from batch_process_county_stats import BatchCountyProcessor, console
import argparse

def main():
    parser = argparse.ArgumentParser(description="Process county statistics for specific scenario")
    parser.add_argument("--scenario", required=True, choices=["historical", "ssp126", "ssp245", "ssp370", "ssp585"])
    parser.add_argument("--workers", "-w", type=int, default=4)
    parser.add_argument("--no-chunking", action="store_true")

    args = parser.parse_args()

    # Initialize processor
    processor = BatchCountyProcessor(
        n_workers=args.workers,
        use_chunked_strategy=not args.no_chunking
    )

    # Override scenario
    processor.scenario = args.scenario
    console.print(f"[bold cyan]Processing scenario: {args.scenario}[/bold cyan]")

    # Run batch processing
    processor.run_batch_processing()

if __name__ == "__main__":
    main()
```

Then run:
```bash
uv run python scripts/batch_process_county_stats_scenario.py --scenario ssp245 --workers 8
```

#### Step 2.2: Complete Stage 2 Processing

Save this as `scripts/process_scenario_stage2.sh`:

```bash
#!/bin/bash
# Stage 2: County Statistics Calculation
# Usage: ./process_scenario_stage2.sh ssp245

set -e

SCENARIO=$1

if [ -z "$SCENARIO" ]; then
    echo "Usage: $0 <scenario>"
    echo "Available scenarios: ssp126, ssp245, ssp370, ssp585"
    exit 1
fi

echo "========================================="
echo "Stage 2: County Statistics Calculation"
echo "Scenario: $SCENARIO"
echo "========================================="

# Check if Zarr files exist
ZARR_DIR="/Volumes/SSD1TB/climate_outputs/zarr"
MISSING=0

for VAR in pr tas tasmax tasmin; do
    for REGION in conus alaska hawaii puerto_rico guam; do
        ZARR_FILE="$ZARR_DIR/$VAR/$REGION/$SCENARIO/${REGION}_${SCENARIO}_${VAR}_daily.zarr"
        if [ ! -d "$ZARR_FILE" ]; then
            echo "❌ Missing: $ZARR_FILE"
            MISSING=$((MISSING+1))
        fi
    done
done

if [ $MISSING -gt 0 ]; then
    echo ""
    echo "ERROR: $MISSING Zarr datasets missing!"
    echo "Run Stage 1 first: ./process_scenario_stage1.sh $SCENARIO"
    exit 1
fi

echo "✅ All Zarr datasets found"
echo ""

# Create temporary scenario-specific processor script
cat > /tmp/process_${SCENARIO}.py << EOF
#!/usr/bin/env python
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / "src"))

from scripts.batch_process_county_stats import BatchCountyProcessor

processor = BatchCountyProcessor(n_workers=8, use_chunked_strategy=True)
processor.scenario = "$SCENARIO"  # Override scenario
processor.run_batch_processing()
EOF

# Run county statistics processing
echo "Starting batch processing..."
uv run python /tmp/process_${SCENARIO}.py

if [ $? -eq 0 ]; then
    echo ""
    echo "========================================="
    echo "✅ Stage 2 Complete for $SCENARIO"
    echo "========================================="
    echo ""
    echo "Output location:"
    echo "  /Volumes/SSD1TB/climate_outputs/stats/"
    echo ""
    echo "Next step: Create unified output"
    echo "  python scripts/create_unified_output.py --scenario $SCENARIO"
else
    echo "❌ Stage 2 failed"
    exit 1
fi

# Cleanup
rm /tmp/process_${SCENARIO}.py
```

### Stage 3: Create Unified Output

Combine all county statistics into a single analysis-ready file.

#### Create Unified Output Script

Save this as `scripts/create_unified_output.py`:

```python
#!/usr/bin/env python
"""
Create unified climate metrics file combining all variables.

This script combines individual county statistics files into a single
time-series format matching the climate_metrics_ssp370.csv structure.

Usage:
    python create_unified_output.py --scenario ssp245
    python create_unified_output.py --scenario ssp585 --output custom_output.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
import argparse
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

console = Console()

def load_county_stats(stats_dir: Path, variable: str, region: str, scenario: str) -> pd.DataFrame:
    """Load county statistics for a specific variable/region/scenario."""

    stats_file = stats_dir / variable / region / scenario / f"{variable}_stats_{region}_{scenario}.csv"

    if not stats_file.exists():
        console.print(f"[yellow]Warning: Missing {stats_file}[/yellow]")
        return None

    try:
        df = pd.read_csv(stats_file)
        return df
    except Exception as e:
        console.print(f"[red]Error loading {stats_file}: {e}[/red]")
        return None

def create_unified_output(scenario: str, output_file: str = None):
    """Create unified climate metrics file for a scenario."""

    console.print(f"\n[bold cyan]Creating Unified Climate Metrics[/bold cyan]")
    console.print(f"Scenario: {scenario}")

    stats_dir = Path("/Volumes/SSD1TB/climate_outputs/stats")

    if output_file is None:
        output_file = f"/Volumes/SSD1TB/climate_metrics_{scenario}.csv"

    # Variables and regions
    variables = ["pr", "tas", "tasmax", "tasmin"]
    regions = ["conus", "alaska", "hawaii", "puerto_rico", "guam"]

    all_data = []
    total_datasets = len(variables) * len(regions)
    loaded = 0

    console.print(f"\nLoading {total_datasets} datasets...")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console
    ) as progress:

        task = progress.add_task("Loading datasets...", total=total_datasets)

        for variable in variables:
            for region in regions:
                progress.update(task, description=f"Loading {region}-{variable}...")

                df = load_county_stats(stats_dir, variable, region, scenario)

                if df is not None:
                    # Add metadata
                    df['variable'] = variable
                    df['region'] = region
                    df['scenario'] = scenario
                    all_data.append(df)
                    loaded += 1

                progress.advance(task)

    if not all_data:
        console.print("[red]No data loaded! Check that Stage 2 is complete.[/red]")
        return

    console.print(f"\n[green]Loaded {loaded}/{total_datasets} datasets[/green]")

    # Combine all data
    console.print("\nCombining datasets...")
    combined = pd.concat(all_data, ignore_index=True)

    # Restructure to match target format
    # Target columns: cid2, year, scenario, name, daysabove1in, daysabove90F, tmaxavg, annual_mean_temp, annual_total_precip

    console.print("Restructuring to time-series format...")

    # This is a simplified transformation - you'll need to customize based on your actual column names
    # The goal is to pivot from annual statistics to year-by-year time series

    # Group by county and create time series
    # (This is a placeholder - actual implementation depends on your data structure)

    unified = combined.copy()

    # Rename columns to match target format
    column_mapping = {
        'county_fips': 'cid2',
        'county_name': 'name',
        # Add more mappings as needed
    }
    unified = unified.rename(columns=column_mapping)

    # Save unified output
    console.print(f"\nSaving to: {output_file}")
    unified.to_csv(output_file, index=False)

    file_size_mb = Path(output_file).stat().st_size / (1024 * 1024)
    console.print(f"\n[bold green]✅ Unified output created![/bold green]")
    console.print(f"  File: {output_file}")
    console.print(f"  Size: {file_size_mb:.1f} MB")
    console.print(f"  Rows: {len(unified):,}")
    console.print(f"  Counties: {unified['cid2'].nunique():,}")

def main():
    parser = argparse.ArgumentParser(
        description="Create unified climate metrics file",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--scenario",
        required=True,
        choices=["historical", "ssp126", "ssp245", "ssp370", "ssp585"],
        help="Climate scenario to process"
    )

    parser.add_argument(
        "--output", "-o",
        help="Output CSV file (default: /Volumes/SSD1TB/climate_metrics_{scenario}.csv)"
    )

    args = parser.parse_args()

    create_unified_output(args.scenario, args.output)

if __name__ == "__main__":
    main()
```

## Complete Scenario Processing Pipeline

### Master Script: Process Entire Scenario

Save this as `scripts/process_complete_scenario.sh`:

```bash
#!/bin/bash
# Complete pipeline: Process entire scenario from NetCDF to final output
# Usage: ./process_complete_scenario.sh ssp245

set -e

SCENARIO=$1

if [ -z "$SCENARIO" ]; then
    echo "Usage: $0 <scenario>"
    echo ""
    echo "Available scenarios:"
    echo "  ssp126  - Low emissions (Paris Agreement target)"
    echo "  ssp245  - Medium emissions (most likely)"
    echo "  ssp370  - Medium-high emissions (already processed)"
    echo "  ssp585  - High emissions (worst case)"
    exit 1
fi

echo "╔════════════════════════════════════════╗"
echo "║   Complete Scenario Processing Pipeline   ║"
echo "║   Scenario: $SCENARIO                      ║"
echo "╚════════════════════════════════════════╝"
echo ""

START_TIME=$(date +%s)

# Stage 1: NetCDF → Zarr Conversion
echo "📊 STAGE 1: NetCDF → Zarr Conversion"
echo "========================================"
./scripts/process_scenario_stage1.sh $SCENARIO

if [ $? -ne 0 ]; then
    echo "❌ Stage 1 failed"
    exit 1
fi

# Stage 2: County Statistics
echo ""
echo "📈 STAGE 2: County Statistics Calculation"
echo "========================================"
./scripts/process_scenario_stage2.sh $SCENARIO

if [ $? -ne 0 ]; then
    echo "❌ Stage 2 failed"
    exit 1
fi

# Stage 3: Unified Output
echo ""
echo "📋 STAGE 3: Create Unified Output"
echo "========================================"
uv run python scripts/create_unified_output.py --scenario $SCENARIO

if [ $? -ne 0 ]; then
    echo "❌ Stage 3 failed"
    exit 1
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
HOURS=$((DURATION / 3600))
MINUTES=$(((DURATION % 3600) / 60))

echo ""
echo "╔════════════════════════════════════════╗"
echo "║   ✅ COMPLETE PIPELINE FINISHED         ║"
echo "║   Scenario: $SCENARIO                      ║"
echo "║   Duration: ${HOURS}h ${MINUTES}m                  ║"
echo "╚════════════════════════════════════════╝"
echo ""
echo "Final output:"
echo "  /Volumes/SSD1TB/climate_metrics_${SCENARIO}.csv"
echo ""
echo "Ready for SLR research analysis!"
```

Make scripts executable:
```bash
chmod +x scripts/process_scenario_stage1.sh
chmod +x scripts/process_scenario_stage2.sh
chmod +x scripts/process_complete_scenario.sh
```

## Usage Examples

### Process SSP2-4.5 (Medium Emissions)

```bash
# Complete pipeline
./scripts/process_complete_scenario.sh ssp245

# Or step-by-step:
./scripts/process_scenario_stage1.sh ssp245  # NetCDF → Zarr
./scripts/process_scenario_stage2.sh ssp245  # County statistics
uv run python scripts/create_unified_output.py --scenario ssp245  # Unified output
```

### Process SSP5-8.5 (High Emissions)

```bash
./scripts/process_complete_scenario.sh ssp585
```

### Process Multiple Scenarios

```bash
# Process all future scenarios
for scenario in ssp245 ssp585 ssp126; do
    echo "Processing $scenario..."
    ./scripts/process_complete_scenario.sh $scenario

    if [ $? -eq 0 ]; then
        echo "✅ $scenario complete"
    else
        echo "❌ $scenario failed"
        break
    fi
done
```

## Expected Processing Times

Based on SSP3-7.0 processing experience:

### Stage 1: NetCDF → Zarr Conversion
- **CONUS** (per variable): 30-60 minutes
- **Alaska** (per variable): 5-10 minutes
- **Hawaii** (per variable): 2-5 minutes
- **Puerto Rico** (per variable): 5-10 minutes
- **Guam** (per variable): 1-2 minutes
- **Total Stage 1**: 3-5 hours (all variables, all regions)

### Stage 2: County Statistics
- **CONUS** (per variable): 1-2 hours (with chunked strategy)
- **Alaska** (per variable): 10-20 minutes
- **Hawaii** (per variable): 5-10 minutes
- **Puerto Rico** (per variable): 15-30 minutes
- **Guam** (per variable): 2-5 minutes
- **Total Stage 2**: 5-8 hours (all variables, all regions)

### Total Pipeline
- **Complete scenario**: 8-13 hours
- **All 3 remaining scenarios**: 24-39 hours

**Optimization Tips**:
- Run overnight or over weekend
- Use `--workers 8` or higher if you have sufficient RAM (32GB+)
- Process scenarios in parallel on different machines if available
- CONUS takes ~70% of total processing time

## Disk Space Requirements

### Per Scenario

| Component | Size | Location |
|-----------|------|----------|
| Raw NetCDF | ~80GB | `/Volumes/SSD1TB/NorESM2-LM/{var}/{scenario}/` |
| Zarr (compressed) | ~8GB | `/Volumes/SSD1TB/climate_outputs/zarr/` |
| County Stats | ~100MB | `/Volumes/SSD1TB/climate_outputs/stats/` |
| Final Output | ~25MB | `/Volumes/SSD1TB/climate_metrics_{scenario}.csv` |
| **Total** | **~88GB** | |

### All Scenarios

| Scenario | Raw NetCDF | Zarr | Stats | Final CSV | Total |
|----------|-----------|------|-------|-----------|-------|
| historical | ~60GB | ~6GB | ~80MB | ~20MB | ~66GB |
| ssp126 | ~80GB | ~8GB | ~100MB | ~25MB | ~88GB |
| ssp245 | ~80GB | ~8GB | ~100MB | ~25MB | ~88GB |
| ssp370 | ~80GB | ~8GB | ~100MB | ~25MB | ~88GB ✅ |
| ssp585 | ~80GB | ~8GB | ~100MB | ~25MB | ~88GB |
| **Total** | **~380GB** | **~38GB** | **~480MB** | **~120MB** | **~420GB** |

**Current Usage**: ~154GB (historical + ssp370)
**Additional Space Needed**: ~266GB (ssp126 + ssp245 + ssp585)

### Space Optimization

After processing and validating, you can:

1. **Archive raw NetCDF** (saves ~300GB):
   ```bash
   cd /Volumes/SSD1TB
   tar -czf NorESM2-LM_backup.tar.gz NorESM2-LM/
   # Move to long-term storage
   mv NorESM2-LM_backup.tar.gz /path/to/backup/
   # Delete originals (after confirming Zarr files work)
   # rm -rf NorESM2-LM/
   ```

2. **Keep only essential files** (final state: ~40GB):
   - Zarr files: ~38GB (needed for re-processing if needed)
   - Final CSV outputs: ~120MB (essential for analysis)
   - Delete county stats CSVs: Recoverable from Zarr if needed

## Troubleshooting

### Stage 1 Fails: NetCDF Not Found

```bash
# Check if NetCDF files exist
ls /Volumes/SSD1TB/NorESM2-LM/pr/ssp245/

# If empty, download data from ESGF (see DATA_ORGANIZATION.md)
```

### Stage 2 Fails: Zarr Not Found

```bash
# Verify Zarr files were created
find /Volumes/SSD1TB/climate_outputs/zarr -name "*ssp245*.zarr"

# If missing, re-run Stage 1
./scripts/process_scenario_stage1.sh ssp245
```

### Out of Memory Errors

```bash
# Reduce workers
uv run python scripts/batch_process_county_stats.py --workers 2

# Or disable chunked strategy
uv run python scripts/batch_process_county_stats.py --no-chunking
```

### Partial Processing Failure

```bash
# Check which datasets succeeded
ls /Volumes/SSD1TB/climate_outputs/stats/*/conus/ssp245/

# Re-run only failed datasets (modify script to skip completed ones)
```

## Validation

After processing, validate the output:

```bash
# Check final output file
python -c "
import pandas as pd
df = pd.read_csv('/Volumes/SSD1TB/climate_metrics_ssp245.csv')
print(f'Rows: {len(df):,}')
print(f'Counties: {df[\"cid2\"].nunique():,}')
print(f'Years: {df[\"year\"].nunique()}')
print(f'Scenarios: {df[\"scenario\"].unique()}')
print('\nColumn summary:')
print(df.describe())
"

# Expected output:
# Rows: ~277,781
# Counties: 3,231
# Years: 86 (2015-2100)
# Scenarios: ['ssp245']
```

## Next Steps After Processing

Once you have multiple scenarios processed:

1. **Comparative Analysis**:
   ```python
   import pandas as pd

   ssp245 = pd.read_csv('/Volumes/SSD1TB/climate_metrics_ssp245.csv')
   ssp370 = pd.read_csv('/Volumes/SSD1TB/climate_metrics_ssp370.csv')
   ssp585 = pd.read_csv('/Volumes/SSD1TB/climate_metrics_ssp585.csv')

   # Compare temperature trends
   scenarios = pd.concat([ssp245, ssp370, ssp585])
   avg_temp = scenarios.groupby(['scenario', 'year'])['annual_mean_temp'].mean()
   ```

2. **SLR Research Integration**: See DATA_ORGANIZATION.md for analysis examples

3. **Visualization**: Create scenario comparison maps and time series

---

**Last Updated**: 2025-10-10
**Maintainer**: Chris Mihiar (chris.mihiar.fs@gmail.com)
**Project**: https://github.com/mihiarc/climate-zarr-slr
