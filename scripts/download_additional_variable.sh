#!/bin/bash
# Download climate variable from NASA NEX-GDDP-CMIP6
# Usage: ./download_additional_variable.sh <variable> <scenario>
#
# Examples:
#   ./download_additional_variable.sh pr historical
#   ./download_additional_variable.sh tas ssp585
#   ./download_additional_variable.sh sfcWind ssp245

set -e

VARIABLE=$1
SCENARIO=$2

if [ -z "$VARIABLE" ] || [ -z "$SCENARIO" ]; then
    echo "Usage: $0 <variable> <scenario>"
    echo ""
    echo "Variables: pr, tas, tasmax, tasmin, sfcWind, hurs, rsds, rlds, huss"
    echo "Scenarios: historical, ssp126, ssp245, ssp370, ssp585"
    exit 1
fi

OUTPUT_DIR="/Volumes/SSD1TB/NorESM2-LM/$VARIABLE/$SCENARIO"
BASE_URL="https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/NorESM2-LM/$SCENARIO/r1i1p1f1/$VARIABLE"

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
echo "Downloading: $VARIABLE / $SCENARIO"
echo "Output: $OUTPUT_DIR"
echo "========================================"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Check if already complete
EXISTING_FILES=$(ls "$OUTPUT_DIR"/*.nc 2>/dev/null | wc -l | tr -d ' ')
if [ "$EXISTING_FILES" -eq "$EXPECTED_FILES" ]; then
    echo "✅ Already complete: $EXISTING_FILES files"
    exit 0
fi

echo "Starting download: $EXPECTED_FILES files ($DATE_RANGE)"
echo ""

COMPLETED=0
FAILED=0

for YEAR in $(seq $START_YEAR $END_YEAR); do
    FILENAME="${VARIABLE}_day_NorESM2-LM_${SCENARIO}_r1i1p1f1_gn_${YEAR}.nc"
    OUTPUT_FILE="$OUTPUT_DIR/$FILENAME"
    
    # Skip if already downloaded
    if [ -f "$OUTPUT_FILE" ]; then
        echo "⏭️  Year $YEAR: Already exists"
        COMPLETED=$((COMPLETED+1))
        continue
    fi
    
    URL="$BASE_URL/$FILENAME"
    
    echo -n "📥 Downloading year $YEAR... "
    
    if curl -f -s -S -o "$OUTPUT_FILE" "$URL"; then
        FILE_SIZE=$(du -h "$OUTPUT_FILE" | awk '{print $1}')
        echo "✅ ($FILE_SIZE)"
        COMPLETED=$((COMPLETED+1))
    else
        echo "❌ Failed"
        FAILED=$((FAILED+1))
        rm -f "$OUTPUT_FILE"
    fi
    
    # Brief pause to avoid overwhelming server
    sleep 1
done

echo ""
echo "========================================"
echo "Download Summary"
echo "========================================"
echo "Completed: $COMPLETED/$EXPECTED_FILES"
echo "Failed: $FAILED"
echo "Output: $OUTPUT_DIR"

if [ $COMPLETED -eq $EXPECTED_FILES ]; then
    echo ""
    echo "✅ Download complete for $VARIABLE/$SCENARIO"
    TOTAL_SIZE=$(du -sh "$OUTPUT_DIR" | awk '{print $1}')
    echo "📊 Total size: $TOTAL_SIZE"
else
    echo ""
    echo "⚠️  Incomplete: $COMPLETED/$EXPECTED_FILES files downloaded"
    exit 1
fi
