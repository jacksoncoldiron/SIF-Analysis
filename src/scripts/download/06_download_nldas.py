#!/usr/bin/env python3
"""
NLDAS-2 Noah Land Surface Model ET Download (CONUS + Iowa + Nebraska, 2015-2024)

Downloads NLDAS-2 Noah monthly actual evapotranspiration (Evap variable) for
the full CONUS, then:
  1. Saves the full CONUS Evap fields to data/processed/nldas/conus/
  2. Clips to Iowa   → data/processed/nldas/iowa/
  3. Clips to Nebraska → data/processed/nldas/nebraska/

The raw NLDAS files already cover all of CONUS at 0.125° resolution, so a
single download serves all three output extents.

PURPOSE:
--------
Actual ET from the Noah LSM is the denominator in the HumanET calculation:
    HumanET = OpenET − NLDAS_Noah_ET

Where OpenET > NLDAS Noah ET, the excess indicates irrigation or other
anthropogenic water additions.

Dataset:  NLDAS Noah Land Surface Model L4 Monthly 0.125 × 0.125 degree V2.0
DOI:      10.5067/WB224IA3PVOJ
Variable: Evap  (Total Evapotranspiration, kg/m² = mm/month)

Expected: 120 raw granules (12 months × 10 years, 2015–2024)

Outputs:
  data/raw/nldas/NLDAS_NOAH0125_M.AYYYYMM.020.nc      (raw CONUS files)
  data/processed/nldas/conus/NLDAS_Evap_YYYYMM.nc     (CONUS Evap layer)
  data/processed/nldas/iowa/NLDAS_Evap_Iowa_YYYYMM.nc
  data/processed/nldas/nebraska/NLDAS_Evap_Nebraska_YYYYMM.nc

Requirements:
  - earthaccess  (pip install earthaccess)
  - ~/.netrc with NASA Earthdata credentials
  - xarray, rioxarray, geopandas, numpy
"""

import sys
import subprocess
from pathlib import Path

# Ensure earthaccess is importable on HPC
target_dir = str(Path.home() / '.local/lib/python3.12/site-packages')
if target_dir not in sys.path:
    sys.path.insert(0, target_dir)

try:
    import earthaccess
except ImportError:
    subprocess.check_call([
        sys.executable, '-m', 'pip', 'install',
        '--target=' + target_dir, 'earthaccess'
    ])
    import earthaccess

import numpy as np
import xarray as xr
import rioxarray as rxr
import geopandas as gpd

# =============================================================================
# Configuration
# =============================================================================

START_YEAR = 2015
END_YEAR   = 2024

# NLDAS CONUS bounding box — broad enough to capture full grid
BBOX = (-125.1, 24.9, -66.9, 53.1)   # west, south, east, north

# Full CONUS NLDAS grid parameters (for alignment verification)
# 464 columns × 224 rows, 0.125° spacing
# Upper-left corner at (−125.0, 53.0); cell centers start at (−124.9375, 52.9375)
CONUS_WIDTH, CONUS_HEIGHT = 464, 224
CONUS_X0, CONUS_Y0       = -124.9375, 52.9375   # center of upper-left cell

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / 'data' / 'raw'       / 'nldas'
CONUS_OUT    = PROJECT_ROOT / 'data' / 'processed' / 'nldas' / 'conus'
IOWA_OUT     = PROJECT_ROOT / 'data' / 'processed' / 'nldas' / 'iowa'
NE_OUT       = PROJECT_ROOT / 'data' / 'processed' / 'nldas' / 'nebraska'
IOWA_AOI     = PROJECT_ROOT / 'data' / 'aoi'       / 'iowa.geojson'
NE_AOI       = PROJECT_ROOT / 'data' / 'aoi'       / 'nebraska.geojson'

for d in [RAW_DIR, CONUS_OUT, IOWA_OUT, NE_OUT]:
    d.mkdir(parents=True, exist_ok=True)

# =============================================================================
# Download raw NLDAS granules
# =============================================================================

auth = earthaccess.login()

temporal   = (f'{START_YEAR}-01-01', f'{END_YEAR}-12-31')
n_expected = (END_YEAR - START_YEAR + 1) * 12

print(f'Searching NLDAS Noah monthly data {START_YEAR}–{END_YEAR}...')
results = earthaccess.search_data(
    doi='10.5067/WB224IA3PVOJ',
    temporal=temporal,
    bounding_box=BBOX,
)
print(f'Found {len(results)} granules (expected {n_expected})')

existing = list(RAW_DIR.glob('NLDAS_NOAH0125_M.A*.nc'))
print(f'Already on disk: {len(existing)} — downloading remainder')

if results:
    downloaded = earthaccess.download(results, local_path=str(RAW_DIR))
    print(f'Downloaded {len(downloaded)} files to {RAW_DIR}')
else:
    print('No new data found.')

# =============================================================================
# Process: extract Evap and save for CONUS, Iowa, Nebraska
# =============================================================================

nc_files = sorted(RAW_DIR.glob('NLDAS_NOAH0125_M.A*.nc'))
print(f'\nProcessing {len(nc_files)} NLDAS files...')

# Load state AOIs
iowa_gdf = gpd.read_file(IOWA_AOI).to_crs('EPSG:4326') if IOWA_AOI.exists() else None
ne_gdf   = gpd.read_file(NE_AOI).to_crs('EPSG:4326')   if NE_AOI.exists()   else None

if iowa_gdf is None:
    print('  WARNING: Iowa AOI not found. Run 00_make_aoi.py first. Skipping Iowa clipping.')
if ne_gdf is None:
    print('  WARNING: Nebraska AOI not found. Run 00_make_aoi.py first. Skipping Nebraska clipping.')


def yyyymm_from_filename(fpath):
    """Extract YYYYMM from NLDAS filename NLDAS_NOAH0125_M.AYYYYMMDD.020.nc."""
    stem = fpath.stem                     # e.g. 'NLDAS_NOAH0125_M.A201501.020'
    date_part = stem.split('.A')[1][:6]   # e.g. '201501'
    return date_part


for fpath in nc_files:
    yyyymm = yyyymm_from_filename(fpath)

    # ── CONUS output ──────────────────────────────────────────────────────────
    conus_out = CONUS_OUT / f'NLDAS_Evap_{yyyymm}.nc'
    iowa_out  = IOWA_OUT  / f'NLDAS_Evap_Iowa_{yyyymm}.nc'
    ne_out    = NE_OUT    / f'NLDAS_Evap_Nebraska_{yyyymm}.nc'

    try:
        ds = xr.open_dataset(fpath)
        da = ds['Evap'].rio.write_crs('EPSG:4326')

        # Save full-CONUS Evap
        if not conus_out.exists():
            da.to_netcdf(conus_out)
            print(f'  CONUS → {conus_out.name}')

        # Clip to Iowa
        if iowa_gdf is not None and not iowa_out.exists():
            clipped = da.rio.clip(iowa_gdf.geometry, iowa_gdf.crs, drop=True)
            clipped.to_netcdf(iowa_out)
            print(f'  Iowa  → {iowa_out.name}')

        # Clip to Nebraska
        if ne_gdf is not None and not ne_out.exists():
            clipped = da.rio.clip(ne_gdf.geometry, ne_gdf.crs, drop=True)
            clipped.to_netcdf(ne_out)
            print(f'  NE    → {ne_out.name}')

        ds.close()

    except Exception as e:
        print(f'  ERROR {fpath.name}: {e}')

# =============================================================================
# Summary
# =============================================================================
print('\nNLDAS processing complete.')
print(f'  Raw files:     {len(nc_files)} in {RAW_DIR}')
print(f'  CONUS Evap:    {len(list(CONUS_OUT.glob("*.nc")))} files in {CONUS_OUT}')
print(f'  Iowa clipped:  {len(list(IOWA_OUT.glob("*.nc")))} files in {IOWA_OUT}')
print(f'  NE clipped:    {len(list(NE_OUT.glob("*.nc")))} files in {NE_OUT}')
print()
print('Next step: run 00_align_spatial_data_conus.ipynb to harmonize all data')
print('to the NLDAS CONUS grid and save to data/processed/conus/')
