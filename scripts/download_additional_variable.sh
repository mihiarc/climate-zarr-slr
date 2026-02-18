#!/bin/bash
################################################################################
# Download NASA CMIP6 Climate Data from NEX-GDDP-CMIP6
################################################################################
#
# QUICK START:
#   1. Make script executable:
#      chmod +x download_additional_variable.sh
#
#   2. Run a test download:
#      ./download_additional_variable.sh ACCESS-CM2 pr historical
#
#   3. Monitor progress in real-time:
#      tail -f /path/to/output/directory/.../*.nc
#
#   4. Download multiple models/variables in parallel:
#      for var in pr tas tasmax tasmin; do
#          ./download_additional_variable.sh ACCESS-CM2 $var historical &
#      done
#      wait  # Wait for all to complete
#
# PREREQUISITES:
#   Before running, ensure you have:
#   - bash 4.0 or later (most systems have this)
#   - curl command (for downloading files)
#   - ~50-80 GB free disk space per model/variable/scenario
#   - Internet connection (recommend 100+ Mbps for parallel downloads)
#
#   Check prerequisites:
#      bash --version              # Should show bash 4.0+
#      curl --version              # Should show curl with HTTP/HTTPS support
#      df -h /path/to/storage      # Check available disk space
#
# INSTALLATION (if curl is missing):
#   macOS:
#      brew install curl
#
#   Ubuntu/Debian:
#      sudo apt-get install curl
#
#   Windows (Git Bash/WSL):
#      Should already have curl installed
#      If not: apt-get install curl
#
#   CentOS/RHEL:
#      sudo yum install curl
#
# EXECUTION OPTIONS:
#   Option 1: Single download (synchronous - waits for completion)
#      ./download_additional_variable.sh ACCESS-CM2 pr historical
#      # Script blocks until download finishes
#
#   Option 2: Background download (asynchronous - continues immediately)
#      ./download_additional_variable.sh ACCESS-CM2 pr historical &
#      # Outputs job number, script runs in background
#      jobs              # Check status of background jobs
#      fg                # Bring job to foreground
#
#   Option 3: Batch downloads (parallel processing)
#      # Download all variables for one model/scenario
#      cd /path/to/script/directory
#      for var in pr tas tasmax tasmin; do
#          ./download_additional_variable.sh ACCESS-CM2 $var historical &
#      done
#      wait  # Wait for all background jobs to finish
#
#   Option 4: Multiple models (use GNU parallel if available)
#      parallel ./download_additional_variable.sh ::: ACCESS-CM2 CanESM5 \
#        ::: pr tas tasmax tasmin \
#        ::: historical ssp585
#
# PERFORMANCE TUNING:
#   For optimal download speed, consider:
#   - Maximum 4-6 parallel downloads recommended
#   - Network speed: 100+ Mbps = ~2-3 MB/s per download
#   - Throttle if seeing network timeouts
#
#   Example: Download at controlled rate
#      for model in ACCESS-CM2 CanESM5; do
#        for var in pr tas; do
#          ./download_additional_variable.sh $model $var historical &
#          sleep 2  # Stagger starts by 2 seconds
#        done
#      done
#      wait
#
# MONITORING & DEBUGGING:
#   Watch download progress:
#      watch -n 5 'du -sh /c/repos/data/climate_download/data/ACCESS-CM2/*/'
#      # Refreshes every 5 seconds
#
#   Check individual file downloads:
#      ls -lh /c/repos/data/climate_download/data/ACCESS-CM2/pr/historical/
#
#   Check if running processes are downloading:
#      ps aux | grep curl
#      ps aux | grep download_additional_variable
#
#   Follow real-time output (if running in background):
#      tail -f output.log  # Requires redirecting stderr/stdout
#
# RESUMING INTERRUPTED DOWNLOADS:
#   If download is interrupted:
#   1. Simply re-run the exact same command
#   2. Script automatically detects existing files and skips them
#   3. Downloads only missing years
#   4. No need to manually clean up partial files
#
#   Example:
#      # First attempt (interrupted at 30%)
#      ./download_additional_variable.sh ACCESS-CM2 pr historical
#      # Ctrl+C to interrupt
#
#      # Resume (picks up where it left off)
#      ./download_additional_variable.sh ACCESS-CM2 pr historical
#
# TROUBLESHOOTING:
#   Problem: "Command not found: curl"
#   Solution: Install curl (see INSTALLATION section above)
#
#   Problem: "Permission denied" when executing script
#   Solution: Run: chmod +x download_additional_variable.sh
#
#   Problem: "No space left on device"
#   Solution: Check disk space with: df -h
#            Either free up space or use custom output directory
#
#   Problem: Slow downloads or timeouts
#   Solution: Try with fewer parallel downloads
#            Check internet connection: speedtest-cli
#            Run at different time (server might be busy)
#
#   Problem: "Already have: X/Y files" but download failed
#   Solution: Some files may be corrupted
#            Delete entire directory and restart: rm -rf /path/to/data
#            Then re-run script
#
#   Problem: Script exits with error after downloading partial years
#   Solution: This is normal - script tracks progress
#            Re-run to resume: ./download_additional_variable.sh [args]
#
# STORAGE ESTIMATES:
#   Single year per variable: ~50-80 MB
#   Historical scenario (65 years): ~3-5 GB per variable
#   Future scenario (86 years): ~4-7 GB per variable
#   All 4 variables for 1 model/scenario: ~20-25 GB
#   All models + all variables + all scenarios: ~300-400 GB
#
#   Example disk space calculation:
#   - 15 models × 4 variables × 5 scenarios = 300 combinations
#   - Average 70 MB per file × 75 files (avg) = 5.25 GB per combo
#   - Total: ~1.6 TB for complete library
#
# NEXT STEPS AFTER DOWNLOAD:
#   1. Verify downloads: Check file count and integrity
#      find /c/repos/data/climate_download/data -name "*.nc" | wc -l
#
#   2. Convert to Zarr (pipeline):
#      python -m climate_zarr.pipeline \
#        --nc-dir /c/repos/data/climate_download/data/ACCESS-CM2 \
#        --region conus
#
#   3. Extract county statistics:
#      See climate_zarr documentation for county processor
#
# SUPPORT & DOCUMENTATION:
#   - Full project README: ../../README.md
#   - Pipeline documentation: ../../docs/
#   - Issue tracker: https://github.com/mihiarc/climate-zarr-slr/issues
#   - NASA CMIP6 info: https://www.ncei.noaa.gov/products/nex-gddp-cmip6
#
################################################################################

set -e
#   Downloads daily climate data from NASA's Globally Downscaled Climate
#   Projections CMIP6 (NEX-GDDP-CMIP6) via AWS S3. This script fetches
#   historical and future scenario data for various climate models and
#   variables, organizing downloads by model/variable/scenario.
#
# USAGE:
#   ./download_additional_variable.sh <model> <variable> <scenario> [output_dir]
#
# REQUIRED ARGUMENTS:
#   model      - Climate model name (e.g., ACCESS-CM2, CanESM5, NorESM2-LM)
#   variable   - Climate variable (pr, tas, tasmax, tasmin, etc.)
#   scenario   - Emissions scenario (historical, ssp126, ssp245, ssp370, ssp585)
#
# OPTIONAL ARGUMENTS:
#   output_dir - Output directory (default: /c/repos/data/climate_download/data)
#
# EXAMPLES:
#   # Download precipitation for ACCESS-CM2 historical scenario
#   ./download_additional_variable.sh ACCESS-CM2 pr historical
#
#   # Download temperature projections for SSP5-8.5 scenario
#   ./download_additional_variable.sh CanESM5 tas ssp585
#
#   # Download to custom directory
#   ./download_additional_variable.sh NorESM2-LM tasmax ssp370 /mnt/climate_data
#
# AVAILABLE MODELS:
#   - ACCESS-CM2
#   - ACCESS-ESM1-5
#   - BCC-CSM2-MR
#   - CanESM5
#   - CNRM-CM6-1
#   - CNRM-ESM2-1
#   - EC-Earth3
#   - GFDL-ESM4
#   - GISS-E2-1-G
#   - INM-CM5-0
#   - IPSL-CM6A-LR
#   - MIROC6
#   - MPI-ESM1-2-LR
#   - MRI-ESM2-0
#   - UKESM1-0-LL
#
# AVAILABLE VARIABLES:
#   - pr       - Precipitation (mm/day)
#   - tas      - Surface air temperature (K)
#   - tasmax   - Maximum daily temperature (K)
#   - tasmin   - Minimum daily temperature (K)
#   - sfcWind  - Surface wind speed (m/s)
#   - hurs     - Relative humidity (%)
#   - rsds     - Downward shortwave radiation (W/m²)
#   - rlds     - Downward longwave radiation (W/m²)
#   - huss     - Specific humidity (kg/kg)
#
# AVAILABLE SCENARIOS:
#   - historical   - Historical observations (1950-2014, 65 years)
#   - ssp126       - SSP1-2.6 Low emissions (2015-2100, 86 years)
#   - ssp245       - SSP2-4.5 Intermediate emissions (2015-2100, 86 years)
#   - ssp370       - SSP3-7.0 High emissions (2015-2100, 86 years)
#   - ssp585       - SSP5-8.5 Highest emissions (2015-2100, 86 years)
#
# DATA CHARACTERISTICS:
#   - Resolution:  0.25° × 0.25° (~28 km at equator)
#   - Format:      NetCDF4 daily data
#   - Ensemble:    r1i1p1f1 (single realization)
#   - Time Zone:   UTC
#   - File size:   ~50-80 MB per year per variable
#
# PARALLEL PROCESSING:
#   Download multiple models/variables in parallel for faster completion:
#
#   # Download all variables for ACCESS-CM2 historical in parallel
#   for var in pr tas tasmax tasmin; do
#       ./download_additional_variable.sh ACCESS-CM2 $var historical &
#   done
#   wait  # Wait for all downloads to complete
#
# TROUBLESHOOTING:
#   - Network errors: Script will retry with 1-second pause between files
#   - Incomplete download: Re-run script; it skips existing files
#   - Permission issues: Ensure output_dir is writable
#   - Slow speeds: Try limiting parallel downloads (max 4-6 recommended)
#
# REQUIREMENTS:
#   - bash 4.0+
#   - curl (for HTTPS downloads)
#   - Standard Unix utilities (ls, seq, du, awk, mkdir)
#   - Internet connection (100+ Mbps recommended for parallel downloads)
#   - ~50-80 GB free disk space per model/variable/scenario combination
#
# AUTHOR:
#   climate-zarr-slr project
#
# LICENSE:
#   MIT
#
################################################################################

set -e

MODEL=$1
VARIABLE=$2
SCENARIO=$3
OUTPUT_BASE_DIR="${4:=/c/repos/data/climate_download/data}"

if [ -z "$MODEL" ] || [ -z "$VARIABLE" ] || [ -z "$SCENARIO" ]; then
    echo "❌ ERROR: Missing required arguments"
    echo ""
    echo "USAGE:"
    echo "  $0 <model> <variable> <scenario> [output_dir]"
    echo ""
    echo "REQUIRED:"
    echo "  model      Climate model (e.g., ACCESS-CM2, CanESM5, NorESM2-LM)"
    echo "  variable   Variable (pr, tas, tasmax, tasmin, sfcWind, hurs, rsds, rlds, huss)"
    echo "  scenario   Scenario (historical, ssp126, ssp245, ssp370, ssp585)"
    echo ""
    echo "OPTIONAL:"
    echo "  output_dir Base output directory (default: /c/repos/data/climate_download/data)"
    echo ""
    echo "EXAMPLES:"
    echo "  # Download precipitation historical data"
    echo "  $0 ACCESS-CM2 pr historical"
    echo ""
    echo "  # Download future projections to custom directory"
    echo "  $0 CanESM5 tas ssp585 /mnt/climate_data"
    echo ""
    echo "AVAILABLE MODELS:"
    echo "  ACCESS-CM2, ACCESS-ESM1-5, BCC-CSM2-MR, CanESM5,"
    echo "  CNRM-CM6-1, CNRM-ESM2-1, EC-Earth3, GFDL-ESM4,"
    echo "  GISS-E2-1-G, INM-CM5-0, IPSL-CM6A-LR, MIROC6,"
    echo "  MPI-ESM1-2-LR, MRI-ESM2-0, UKESM1-0-LL"
    echo ""
    echo "AVAILABLE VARIABLES:"
    echo "  pr (Precipitation, mm/day)"
    echo "  tas (Temperature, K)"
    echo "  tasmax (Max temp, K)"
    echo "  tasmin (Min temp, K)"
    echo "  sfcWind (Wind speed, m/s)"
    echo "  hurs (Humidity, %)"
    echo "  rsds (Shortwave radiation, W/m²)"
    echo "  rlds (Longwave radiation, W/m²)"
    echo "  huss (Specific humidity, kg/kg)"
    echo ""
    echo "AVAILABLE SCENARIOS:"
    echo "  historical (1950-2014, 65 years)"
    echo "  ssp126 (2015-2100, 86 years) - Low emissions"
    echo "  ssp245 (2015-2100, 86 years) - Intermediate emissions"
    echo "  ssp370 (2015-2100, 86 years) - High emissions"
    echo "  ssp585 (2015-2100, 86 years) - Very high emissions"
    echo ""
    exit 1
fi

OUTPUT_DIR="$OUTPUT_BASE_DIR/$MODEL/$VARIABLE/$SCENARIO"
BASE_URL="https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/$MODEL/$SCENARIO/r1i1p1f1/$VARIABLE"

# Set year range and expected file count based on scenario
if [ "$SCENARIO" = "historical" ]; then
    START_YEAR=1950
    END_YEAR=2014
    EXPECTED_FILES=65
    DATE_RANGE="1950-2014"
else
    START_YEAR=2015
    END_YEAR=2100
    EXPECTED_FILES=86
    DATE_RANGE="2015-2100"
fi

echo "========================================"
echo "📊 NASA CMIP6 Climate Data Downloader"
echo "========================================"
echo "Model:    $MODEL"
echo "Variable: $VARIABLE"
echo "Scenario: $SCENARIO"
echo "Output:   $OUTPUT_DIR"
echo "========================================"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Check if already complete
EXISTING_FILES=$(ls "$OUTPUT_DIR"/*.nc 2>/dev/null | wc -l | tr -d ' ')
if [ "$EXISTING_FILES" -eq "$EXPECTED_FILES" ]; then
    echo "✅ ALREADY COMPLETE"
    echo "   Found $EXISTING_FILES/$EXPECTED_FILES files"
    echo "   Total size: $(du -sh "$OUTPUT_DIR" | awk '{print $1}')"
    exit 0
fi

# Calculate progress from existing files
SKIPPED=$EXISTING_FILES
TOTAL_EXPECTED=$EXPECTED_FILES
REMAINING=$((TOTAL_EXPECTED - SKIPPED))

if [ $SKIPPED -gt 0 ]; then
    echo "ℹ️  RESUMING DOWNLOAD"
    echo "   Already have: $SKIPPED/$TOTAL_EXPECTED files"
    echo "   Remaining:    $REMAINING files"
else
    echo "🔄 STARTING DOWNLOAD"
    echo "   Expected:     $TOTAL_EXPECTED files ($DATE_RANGE)"
    echo "   Total data:   ~$(($TOTAL_EXPECTED * 60)) MB (approximate)"
fi

echo ""
echo "Progress:"
echo ""

COMPLETED=$SKIPPED
FAILED=0
PERCENT_DONE=$((SKIPPED * 100 / TOTAL_EXPECTED))

for YEAR in $(seq $START_YEAR $END_YEAR); do
    FILENAME="${VARIABLE}_day_${MODEL}_${SCENARIO}_r1i1p1f1_gn_${YEAR}.nc"
    OUTPUT_FILE="$OUTPUT_DIR/$FILENAME"
    
    # Skip if already downloaded
    if [ -f "$OUTPUT_FILE" ]; then
        COMPLETED=$((COMPLETED+1))
        continue
    fi
    
    URL="$BASE_URL/$FILENAME"
    PERCENT_DONE=$((COMPLETED * 100 / TOTAL_EXPECTED))
    
    printf "  [%3d%%] Year %4d... " "$PERCENT_DONE" "$YEAR"
    
    if curl -f -s -S -o "$OUTPUT_FILE" "$URL"; then
        FILE_SIZE=$(du -h "$OUTPUT_FILE" | awk '{print $1}')
        echo "✅ ($FILE_SIZE)"
        COMPLETED=$((COMPLETED+1))
    else
        echo "❌ FAILED"
        FAILED=$((FAILED+1))
        rm -f "$OUTPUT_FILE"
    fi
    
    # Brief pause to avoid overwhelming server
    sleep 1
done

echo ""
echo "========================================"
echo "📋 DOWNLOAD SUMMARY"
echo "========================================"
echo "Completed:  $((COMPLETED - SKIPPED))/$REMAINING new files"
echo "Reused:     $SKIPPED existing files"
echo "Total:      $COMPLETED/$TOTAL_EXPECTED files"
echo "Failed:     $FAILED"
echo "Output:     $OUTPUT_DIR"
echo "Total size: $(du -sh "$OUTPUT_DIR" | awk '{print $1}')"
echo "========================================"

if [ $COMPLETED -eq $TOTAL_EXPECTED ]; then
    echo ""
    echo "✅ SUCCESS: Download complete!"
    echo ""
    echo "Next steps:"
    echo "  1. Verify data quality: Check file integrity and date coverage"
    echo "  2. Process with pipeline: Use climate_zarr pipeline to convert to Zarr"
    echo "  3. Extract statistics: Generate county-level climate statistics"
    echo ""
    echo "For more info: See README.md or run 'climate-zarr --help'"
else
    echo ""
    echo "⚠️  WARNING: Download incomplete!"
    echo "   $FAILED files failed to download"
    echo ""
    echo "Retry options:"
    echo "  1. Run again to resume: $0 $MODEL $VARIABLE $SCENARIO"
    echo "  2. Check network: Verify internet connection"
    echo "  3. Try later: Server might be temporarily unavailable"
    echo ""
    exit 1
fi
