#!/usr/bin/env bash
#
# Clean up corrupted SSP585 zarr stores and hidden macOS resource fork files
#

set -e

ZARR_BASE="/Volumes/SSD1TB/climate_outputs/zarr"

echo "🧹 Cleaning up corrupted SSP585 zarr stores..."
echo "================================================"
echo

# Variables to clean
VARIABLES=("pr" "tas" "tasmax" "tasmin")
REGIONS=("conus" "alaska" "hawaii" "puerto_rico" "guam")

for var in "${VARIABLES[@]}"; do
    for region in "${REGIONS[@]}"; do
        zarr_dir="$ZARR_BASE/$var/$region/ssp585"

        if [ -d "$zarr_dir" ]; then
            echo "📁 Checking $var/$region/ssp585..."

            # Find any .zarr directories
            zarr_stores=$(find "$zarr_dir" -maxdepth 1 -name "*.zarr" -type d 2>/dev/null || true)

            if [ -n "$zarr_stores" ]; then
                echo "  🗑️  Removing zarr stores..."
                find "$zarr_dir" -maxdepth 1 -name "*.zarr" -type d -exec rm -rf {} \; 2>/dev/null || true
            fi

            # Remove hidden resource fork files
            hidden_files=$(find "$zarr_dir" -name "._*" 2>/dev/null | wc -l | tr -d ' ')
            if [ "$hidden_files" -gt 0 ]; then
                echo "  🗑️  Removing $hidden_files hidden files..."
                find "$zarr_dir" -name "._*" -delete 2>/dev/null || true
            fi

            # Remove .DS_Store files
            find "$zarr_dir" -name ".DS_Store" -delete 2>/dev/null || true

            echo "  ✅ Cleaned"
        fi
    done
done

echo
echo "✅ Cleanup complete!"
echo
echo "Next steps:"
echo "  1. Run: ./scripts/process_scenario_stage1.sh ssp585"
echo "  2. Monitor progress with: tail -f logs/stage1_ssp585_*.log"
