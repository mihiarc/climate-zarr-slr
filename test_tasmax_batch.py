#!/usr/bin/env python
"""Test opening multiple tasmax SSP585 files to simulate stacking."""

import xarray as xr
from pathlib import Path
import sys

data_dir = Path("/Volumes/SSD1TB/NorESM2-LM/tasmax/ssp585/")

# Get all .nc files (this is what glob does)
nc_files = sorted(list(data_dir.glob("*.nc")))

print(f"📁 Found {len(nc_files)} .nc files")
print(f"📄 First file: {nc_files[0].name}")
print(f"📄 Last file: {nc_files[-1].name}")

# Check for any .corrupted files
corrupted_files = list(data_dir.glob("*.corrupted"))
print(f"\n⚠️  Found {len(corrupted_files)} .corrupted files:")
for f in corrupted_files:
    print(f"   - {f.name}")

# Test opening first 3 files
print(f"\n🔬 Testing first 3 files...")
for i, nc_file in enumerate(nc_files[:3]):
    try:
        ds = xr.open_dataset(nc_file)
        print(f"✅ {i+1}. {nc_file.name} - OK")
        ds.close()
    except Exception as e:
        print(f"❌ {i+1}. {nc_file.name} - ERROR: {e}")

# Test xr.open_mfdataset (what stack uses internally)
print(f"\n🔗 Testing xr.open_mfdataset with first 3 files...")
try:
    ds = xr.open_mfdataset(nc_files[:3], combine='by_coords')
    print(f"✅ Successfully opened multi-file dataset")
    print(f"📊 Combined shape: {ds.dims}")
    ds.close()
except Exception as e:
    print(f"❌ Error with open_mfdataset: {type(e).__name__}: {e}")
