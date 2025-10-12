#!/usr/bin/env python
"""Test opening tasmax SSP585 file with xarray."""

import xarray as xr
import sys

file_path = "/Volumes/SSD1TB/NorESM2-LM/tasmax/ssp585/tasmax_day_NorESM2-LM_ssp585_r1i1p1f1_gn_2015.nc"

try:
    print(f"🔍 Attempting to open: {file_path}")
    ds = xr.open_dataset(file_path)
    print("✅ File opened successfully!")
    print("\n📊 Dataset info:")
    print(ds)
    ds.close()
    sys.exit(0)
except Exception as e:
    print(f"❌ Error: {type(e).__name__}")
    print(f"📝 Message: {e}")
    sys.exit(1)
