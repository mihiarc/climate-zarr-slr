#!/bin/bash
# Complete Stage 2: County statistics for entire scenario (Non-interactive)
# Usage: ./process_scenario_stage2.sh ssp585

set -e  # Exit on error

SCENARIO=$1

if [ -z "$SCENARIO" ]; then
    echo "Usage: $0 <scenario>"
    echo "Available scenarios: historical, ssp126, ssp245, ssp585"
    exit 1
fi

echo "========================================="
echo "Stage 2: County Statistics Processing"
echo "Scenario: $SCENARIO (Non-interactive mode)"
echo "========================================="

TOTAL_DATASETS=20  # 4 variables × 5 regions
COMPLETED=0
FAILED=0

# Create log directory
mkdir -p logs

LOG_FILE="logs/stage2_${SCENARIO}_$(date +%Y%m%d_%H%M%S).log"
echo "Logging to: $LOG_FILE"

# Process each variable-region combination
for VAR in pr tas tasmax tasmin; do
    for REGION in conus alaska hawaii puerto_rico guam; do
        echo ""
        echo "[$((COMPLETED+FAILED+1))/$TOTAL_DATASETS] Processing $REGION - $VAR ($SCENARIO)..."

        ZARR_PATH="/Volumes/SSD1TB/climate_outputs/zarr/$VAR/$REGION/$SCENARIO/${REGION}_${SCENARIO}_${VAR}_daily.zarr"
        OUTPUT_CSV="/Volumes/SSD1TB/climate_outputs/stats/$VAR/${REGION}_${SCENARIO}_${VAR}_county_stats.csv"

        # Check if zarr store exists
        if [ ! -d "$ZARR_PATH" ]; then
            echo "⚠️  Zarr store not found: $ZARR_PATH (skipping)"
            FAILED=$((FAILED+1))
            continue
        fi

        # Check if already processed
        if [ -f "$OUTPUT_CSV" ]; then
            echo "✅ Already exists, skipping: $OUTPUT_CSV"
            COMPLETED=$((COMPLETED+1))
            continue
        fi

        # Create output directory
        mkdir -p "$(dirname "$OUTPUT_CSV")"

        # Process county statistics
        echo "  Calculating county statistics..." | tee -a "$LOG_FILE"
        if uv run climate-zarr county-stats \
            "$ZARR_PATH" \
            "$REGION" \
            -v "$VAR" \
            -s "$SCENARIO" \
            -o "$OUTPUT_CSV" \
            --workers 4 >> "$LOG_FILE" 2>&1; then

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
echo "✅ Stage 2 Complete for $SCENARIO"
echo "Processed: $COMPLETED/$TOTAL_DATASETS datasets"
echo "Failed: $FAILED datasets"
echo "========================================="
echo ""

if [ $FAILED -gt 0 ]; then
    echo "⚠️  Check log file for failed datasets: $LOG_FILE"
fi

echo "County statistics available in: /Volumes/SSD1TB/climate_outputs/stats/"
