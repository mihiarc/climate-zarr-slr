#!/usr/bin/env python3
"""
Inventory local raw NetCDF data vs AWS NEX-GDDP-CMIP6 availability
"""

import os
from pathlib import Path
import subprocess

def get_file_count_and_size(directory):
    """Get count and total size of NetCDF files in directory."""
    if not os.path.exists(directory):
        return 0, "N/A", None, None

    nc_files = sorted(Path(directory).glob("*.nc"))
    if not nc_files:
        return 0, "N/A", None, None

    count = len(nc_files)

    # Get total size
    result = subprocess.run(
        ["du", "-sh", directory],
        capture_output=True,
        text=True
    )
    size = result.stdout.split()[0] if result.returncode == 0 else "N/A"

    # Extract years from filenames
    years = []
    for f in nc_files:
        year_str = ''.join(filter(str.isdigit, f.stem[-4:]))
        if year_str and len(year_str) == 4:
            years.append(int(year_str))

    first_year = min(years) if years else None
    last_year = max(years) if years else None

    return count, size, first_year, last_year

def main():
    base_path = Path("/Volumes/SSD1TB/NorESM2-LM")

    variables = ["pr", "tas", "tasmax", "tasmin"]
    scenarios = ["ssp126", "ssp245", "ssp370", "ssp585"]

    print("=" * 80)
    print("LOCAL DATA INVENTORY - NorESM2-LM")
    print("=" * 80)
    print()

    total_files = 0

    inventory = {}

    for var in variables:
        print(f"\n📊 Variable: {var}")
        print("-" * 80)

        inventory[var] = {}

        for scenario in scenarios:
            dir_path = base_path / var / scenario
            count, size, first, last = get_file_count_and_size(dir_path)

            inventory[var][scenario] = {
                'count': count,
                'size': size,
                'first_year': first,
                'last_year': last,
                'exists': count > 0
            }

            if count > 0:
                year_range = f"{first}-{last}" if first and last else "unknown"
                status = "✅"
                total_files += count
                print(f"  {status} {scenario:8s}: {count:3d} files, {size:>6s} total, years {year_range}")
            else:
                status = "❌"
                print(f"  {status} {scenario:8s}: NOT FOUND")

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    # Count complete scenarios
    complete_scenarios = []
    incomplete_scenarios = []

    for scenario in scenarios:
        vars_present = sum(1 for var in variables if inventory[var][scenario]['exists'])
        if vars_present == 4:
            complete_scenarios.append(scenario)
        elif vars_present > 0:
            incomplete_scenarios.append(scenario)

    print(f"\n✅ Complete scenarios (all 4 variables): {', '.join(complete_scenarios) if complete_scenarios else 'None'}")
    if incomplete_scenarios:
        print(f"⚠️  Incomplete scenarios: {', '.join(incomplete_scenarios)}")

    missing_scenarios = [s for s in scenarios if s not in complete_scenarios and s not in incomplete_scenarios]
    if missing_scenarios:
        print(f"❌ Missing scenarios: {', '.join(missing_scenarios)}")

    print(f"\n📁 Total NetCDF files: {total_files}")
    print("💾 Total storage used: Check individual sizes above")

    # Expected files per scenario
    expected_per_scenario = 86  # Files per variable (2015-2100)
    expected_total = len(variables) * len(scenarios) * expected_per_scenario

    print(f"\n📈 Completeness: {total_files}/{expected_total} expected files ({100*total_files/expected_total:.1f}%)")

    # AWS availability
    print("\n" + "=" * 80)
    print("AWS NEX-GDDP-CMIP6 AVAILABILITY")
    print("=" * 80)
    print("\nNASA NEX-GDDP-CMIP6 on AWS S3 provides:")
    print("  • Bucket: s3://nex-gddp-cmip6")
    print("  • Region: us-west-2")
    print("  • Model: NorESM2-LM")
    print("  • Variables: pr, tas, tasmax, tasmin, and more")
    print("  • Scenarios: ssp126, ssp245, ssp370, ssp585")
    print("  • Time range: 2015-2100 (86 years)")
    print("  • Format: NetCDF4 with CF-1.7 metadata")
    print("  • Resolution: 0.25° × 0.25° (downscaled from ~1°)")

    print("\n📥 Download URL pattern:")
    print("  https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/")
    print("    NorESM2-LM/{scenario}/r1i1p1f1/{variable}/")
    print("      {variable}_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}.nc")

    print("\n" + "=" * 80)

    # Identify what's missing
    print("\nMISSING DATA ITEMS:")
    print("=" * 80)
    missing_items = []

    for var in variables:
        for scenario in scenarios:
            info = inventory[var][scenario]
            if not info['exists']:
                missing_items.append((var, scenario, "Entire scenario missing"))
            elif info['count'] != expected_per_scenario:
                missing_count = expected_per_scenario - info['count']
                missing_items.append((var, scenario, f"{missing_count} files missing (have {info['count']}/{expected_per_scenario})"))

    if missing_items:
        for var, scenario, reason in missing_items:
            print(f"  ⚠️  {var}/{scenario}: {reason}")
    else:
        print("  ✅ No missing data - all scenarios complete!")

    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()
