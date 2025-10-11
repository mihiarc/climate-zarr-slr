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

        # Check if input directory exists
        if [ ! -d "$INPUT_DIR" ]; then
            echo "❌ Input directory not found: $INPUT_DIR"
            exit 1
        fi

        # Create output directory
        mkdir -p "$(dirname "$OUTPUT_ZARR")"

        # Convert NetCDF to Zarr (auto-answer yes to prompts)
        yes | uv run climate-zarr create-zarr \
            "$INPUT_DIR" \
            -o "$OUTPUT_ZARR" \
            --region "$REGION" \
            --compression zstd \
            --compression-level 5 \
            --concat-dim time

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
echo "  ./scripts/process_scenario_stage2.sh $SCENARIO"
