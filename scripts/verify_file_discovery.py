#!/usr/bin/env python
"""
Test the robust file discovery utility with the tasmax/ssp585 directory.
"""

from pathlib import Path
from climate_zarr.utils.file_discovery import discover_netcdf_files, get_netcdf_info

# Test directory
test_dir = Path("/Volumes/SSD1TB/NorESM2-LM/tasmax/ssp585/")

print("=" * 70)
print("Testing Robust File Discovery Utility")
print("=" * 70)
print()

# Discover files
files = discover_netcdf_files(
    directory=test_dir,
    pattern="*.nc",
    validate=True,
    verbose=True,
    fail_on_invalid=False
)

print()
print("=" * 70)
print(f"✅ Result: Found {len(files)} valid NetCDF files")
print("=" * 70)
print()

# Show first and last file info
if files:
    print("First file info:")
    info = get_netcdf_info(files[0])
    print(f"  Name: {files[0].name}")
    print(f"  Size: {info['size_mb']:.1f} MB")
    print(f"  Dims: {info['dims']}")
    print(f"  Vars: {info['data_vars']}")

    print()
    print("Last file info:")
    info = get_netcdf_info(files[-1])
    print(f"  Name: {files[-1].name}")
    print(f"  Size: {info['size_mb']:.1f} MB")
    print(f"  Dims: {info['dims']}")
    print(f"  Vars: {info['data_vars']}")
