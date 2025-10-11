#!/usr/bin/env python3
"""Scan all NetCDF files for corruption and fix from AWS S3."""

import sys
from pathlib import Path
import xarray as xr
import subprocess

base_path = Path("/Volumes/SSD1TB/NorESM2-LM")
base_url = "https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/NorESM2-LM"

variables = ["pr", "tas", "tasmax", "tasmin"]
scenarios = ["ssp126", "ssp245", "ssp370", "ssp585"]

print("=" * 70)
print("COMPREHENSIVE CORRUPTION SCAN")
print("=" * 70)
print()

corrupted_files = []
total_files = 0

# Scan all files
for var in variables:
    for scenario in scenarios:
        var_dir = base_path / var / scenario
        if not var_dir.exists():
            continue

        nc_files = list(var_dir.glob("*.nc"))
        for nc_file in nc_files:
            total_files += 1
            try:
                ds = xr.open_dataset(nc_file)
                ds.close()
            except Exception as e:
                error_msg = str(e)[:80]
                print(f"❌ CORRUPTED: {var}/{scenario}/{nc_file.name}")
                print(f"   Error: {error_msg}")
                corrupted_files.append((var, scenario, nc_file.name, nc_file))

print()
print("=" * 70)
print(f"SCAN COMPLETE: {total_files} files scanned")
print(f"Found {len(corrupted_files)} corrupted files")
print("=" * 70)
print()

if not corrupted_files:
    print("✅ No corruptions found - all files are healthy!")
    sys.exit(0)

# Fix corrupted files
print(f"Attempting to fix {len(corrupted_files)} corrupted files from AWS S3...")
print()

fixed = 0
failed = 0

for var, scenario, filename, local_path in corrupted_files:
    print(f"Fixing {var}/{scenario}/{filename}...")

    # Backup corrupted file
    backup_path = str(local_path) + ".corrupted"
    try:
        local_path.rename(backup_path)
        print(f"  ✓ Backed up to {backup_path}")
    except Exception as e:
        print(f"  ⚠️ Could not backup: {e}")

    # Construct download URL
    aws_url = f"{base_url}/{scenario}/r1i1p1f1/{var}/{filename}"

    # Download replacement
    try:
        result = subprocess.run(
            ["curl", "-f", "-s", "-S", "-o", str(local_path), aws_url],
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode == 0:
            # Verify downloaded file
            try:
                ds = xr.open_dataset(local_path)
                ds.close()
                print("  ✅ Downloaded and verified")
                fixed += 1
            except Exception as e:
                print(f"  ❌ Downloaded but still corrupted: {str(e)[:60]}")
                failed += 1
        else:
            print(f"  ❌ Download failed: {result.stderr[:100]}")
            failed += 1
    except subprocess.TimeoutExpired:
        print("  ❌ Download timeout")
        failed += 1
    except Exception as e:
        print(f"  ❌ Error: {str(e)[:100]}")
        failed += 1

print()
print("=" * 70)
print("REPAIR SUMMARY")
print("=" * 70)
print(f"Fixed: {fixed}/{len(corrupted_files)}")
print(f"Failed: {failed}/{len(corrupted_files)}")

if failed > 0:
    print()
    print("⚠️ Some files could not be fixed automatically")
    sys.exit(1)
else:
    print()
    print("✅ All corrupted files have been repaired!")
    sys.exit(0)
