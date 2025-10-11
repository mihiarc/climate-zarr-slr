#!/bin/bash
# Complete Stage 1: NetCDF → Zarr conversion for entire scenario (Non-interactive)
# Usage: ./process_scenario_stage1_noninteractive.sh ssp245

set -e  # Exit on error

SCENARIO=$1

if [ -z "$SCENARIO" ]; then
    echo "Usage: $0 <scenario>"
    echo "Available scenarios: ssp126, ssp245, ssp585"
    exit 1
fi

echo "========================================="
echo "Stage 1: NetCDF → Zarr Conversion"
echo "Scenario: $SCENARIO (Non-interactive mode)"
echo "========================================="

TOTAL_DATASETS=20  # 4 variables × 5 regions
COMPLETED=0
FAILED=0

# Create log directory
mkdir -p logs

LOG_FILE="logs/stage1_${SCENARIO}_$(date +%Y%m%d_%H%M%S).log"
echo "Logging to: $LOG_FILE"

for VAR in pr tas tasmax tasmin; do
    for REGION in conus alaska hawaii puerto_rico guam; do
        echo ""
        echo "[$((COMPLETED+FAILED+1))/$TOTAL_DATASETS] Processing $REGION - $VAR ($SCENARIO)..."

        INPUT_DIR="/Volumes/SSD1TB/NorESM2-LM/$VAR/$SCENARIO/"
        OUTPUT_ZARR="/Volumes/SSD1TB/climate_outputs/zarr/$VAR/$REGION/$SCENARIO/${REGION}_${SCENARIO}_${VAR}_daily.zarr"

        # Check if already exists
        if [ -f "$OUTPUT_ZARR/.zarray" ]; then
            echo "✅ Already exists, skipping: $OUTPUT_ZARR"
            COMPLETED=$((COMPLETED+1))
            continue
        fi

        # Check if input directory exists
        if [ ! -d "$INPUT_DIR" ]; then
            echo "⚠️  Input directory not found: $INPUT_DIR (skipping)"
            FAILED=$((FAILED+1))
            continue
        fi

        # Create output directory
        mkdir -p "$(dirname "$OUTPUT_ZARR")"

        # Convert NetCDF to Zarr (non-interactive mode - default as of 2025)
        echo "  Converting with climate-zarr..." | tee -a "$LOG_FILE"
        if uv run climate-zarr create-zarr \
            "$INPUT_DIR" \
            -o "$OUTPUT_ZARR" \
            --region "$REGION" \
            --compression zstd \
            --compression-level 5 \
            --concat-dim time >> "$LOG_FILE" 2>&1; then

            COMPLETED=$((COMPLETED+1))
            echo "✅ [$COMPLETED/$TOTAL_DATASETS] $REGION-$VAR completed"
        else
            FAILED=$((FAILED+1))
            echo "❌ $REGION-$VAR failed (see log for details)"
            echo "Failed: $REGION-$VAR at $(date)" >> "$LOG_FILE"
        fi

        # Brief pause to avoid overwhelming the system
        sleep 2
    done
done

echo ""
echo "========================================="
echo "✅ Stage 1 Complete for $SCENARIO"
echo "Processed: $COMPLETED/$TOTAL_DATASETS datasets"
echo "Failed: $FAILED datasets"
echo "========================================="
echo ""

if [ $FAILED -gt 0 ]; then
    echo "⚠️  Check log file for failed datasets: $LOG_FILE"
fi

echo "Next step: Run Stage 2 (County Statistics)"
echo "  ./scripts/process_scenario_stage2.sh $SCENARIO"
