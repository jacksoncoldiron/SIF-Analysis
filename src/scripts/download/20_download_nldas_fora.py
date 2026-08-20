#!/usr/bin/env python3
"""
NLDAS-2 FORA Atmospheric Forcing Download (CONUS, 2015-2024)

Downloads NLDAS-2 monthly atmospheric forcing files (NLDAS_FORA0125_M) for
the full CONUS domain and extracts the variables needed to compute:

  - Tair   — 2-m air temperature (K)
  - Qair   — specific humidity (kg/kg)
  - PSurf  — surface pressure (Pa)
  - Wind_E — zonal wind component (m/s)
  - Wind_N — meridional wind component (m/s)
  - SWdown — downward shortwave radiation (W/m²)
  - Rainf  — total precipitation (kg/m²)
  - PotEvap — potential evaporation (kg/m²)

NOTE ON VARIABLE NAMES: the GES DISC NetCDF distribution of NLDAS_FORA0125_M
uses descriptive names (Tair, Qair, PSurf, Wind_E, Wind_N, SWdown, ...), NOT
the GRIB short names (TMP, SPFH, PRES, UGRD, VGRD, DSWRF). An earlier version
of this script used the GRIB names and silently skipped every file.

These are used in downstream notebooks to compute:
  - Wind speed: sqrt(Wind_E² + Wind_N²)
  - VPD: es(Tair) − ea(Qair, PSurf)  [kPa]
    where es = 0.6108 * exp(17.27 * T_C / (T_C + 237.3))   [kPa]
          ea = Qair * PSurf / (0.622 + 0.378 * Qair)        [kPa]
          T_C = Tair − 273.15

Dataset:  NLDAS Forcing L4 Hourly 0.125×0.125 degree V2.0 (monthly aggregate)
Product:  NLDAS_FORA0125_M.002
Short name: NLDAS_FORA0125_M
GES DISC: https://hydro1.gesdisc.eosdis.nasa.gov/data/NLDAS/NLDAS_FORA0125_M.002/

NOTE: NASA Earthdata credentials are required (~/.netrc with urs.earthdata.nasa.gov).
Set up via: earthaccess.login() or write manually:
    echo 'machine urs.earthdata.nasa.gov login <user> password <pass>' >> ~/.netrc
    chmod 600 ~/.netrc

Output:
  data/raw/NLDAS_FORA/NLDAS_FORA0125_M.AYYYYMM.020.nc   — raw monthly files
  data/processed/conus/nldas_fora/FORA_YYYYMM.nc         — clipped to CONUS analysis grid
      Variables: Tair, Qair, PSurf, Wind_E, Wind_N, SWdown, Rainf, PotEvap, LWdown
"""

import sys
from pathlib import Path

try:
    import earthaccess
except ImportError:
    sys.exit(
        "ERROR: 'earthaccess' is not installed in this environment.\n"
        "Install it via conda:  conda install -c conda-forge earthaccess\n"
        "Or check your environment's package manager."
    )

import numpy as np
import xarray as xr

# =============================================================================
# Configuration
# =============================================================================

START_YEAR = 2015
END_YEAR   = 2024

# Full NLDAS CONUS bounding box
BBOX = (-125.1, 24.9, -66.9, 53.1)   # west, south, east, north

# FORA variables to extract (GES DISC NetCDF names — see module docstring)
FORA_VARS = ['Tair', 'Qair', 'PSurf', 'Wind_E', 'Wind_N', 'SWdown',
             'Rainf', 'PotEvap', 'LWdown']

# Clipped CONUS analysis grid (must match 01_/02_/03_ notebooks)
CONUS_LON = np.arange(-124.6875, -84.0625,  0.125)   # 325 longitudes
CONUS_LAT = np.arange(  49.3125,  25.6875, -0.125)   # 189 latitudes (N→S)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / 'data' / 'raw'       / 'NLDAS_FORA'
PROC_DIR     = PROJECT_ROOT / 'data' / 'processed' / 'conus' / 'nldas_fora'

for d in [RAW_DIR, PROC_DIR]:
    d.mkdir(parents=True, exist_ok=True)

print('NLDAS FORA download script')
print('Raw output   :', RAW_DIR)
print('Proc output  :', PROC_DIR)
print('Years        :', START_YEAR, '-', END_YEAR)
print('Variables    :', FORA_VARS)
print()

# =============================================================================
# Step 1 — Download raw monthly FORA granules via earthaccess
# =============================================================================

auth = earthaccess.login()

temporal   = (f'{START_YEAR}-01-01', f'{END_YEAR}-12-31')
n_expected = (END_YEAR - START_YEAR + 1) * 12

print(f'Searching NLDAS_FORA0125_M data {START_YEAR}–{END_YEAR}...')
results = earthaccess.search_data(
    short_name='NLDAS_FORA0125_M',
    temporal=temporal,
    bounding_box=BBOX,
)
print(f'Found {len(results)} granules (expected {n_expected})')

existing = list(RAW_DIR.glob('NLDAS_FORA0125_M.A*.nc'))
print(f'Already on disk: {len(existing)} — downloading remainder')

if results:
    downloaded = earthaccess.download(results, local_path=str(RAW_DIR))
    print(f'Downloaded {len(downloaded)} files to {RAW_DIR}')
else:
    print('No new data found — skipping download.')

# =============================================================================
# Step 2 — Process: extract FORA variables, clip to analysis grid, save
# =============================================================================

nc_files = sorted(RAW_DIR.glob('NLDAS_FORA0125_M.A*.nc'))
print(f'\nProcessing {len(nc_files)} FORA files...')


def _yyyymm_from_fname(fpath: Path) -> str:
    """Extract YYYYMM from NLDAS_FORA0125_M.AYYYYMMDD.020.nc filename."""
    stem = fpath.stem                   # e.g. 'NLDAS_FORA0125_M.A201501.020'
    return stem.split('.A')[1][:6]      # e.g. '201501'


_n_processed = 0
_n_skipped   = 0
_n_error     = 0

for fpath in nc_files:
    try:
        yyyymm = _yyyymm_from_fname(fpath)
    except (IndexError, ValueError):
        print(f'  WARNING: could not parse date from {fpath.name} — skipping')
        _n_skipped += 1
        continue

    year = int(yyyymm[:4])
    if year < START_YEAR or year > END_YEAR:
        _n_skipped += 1
        continue

    dst_path = PROC_DIR / f'FORA_{yyyymm}.nc'
    if dst_path.exists():
        _n_skipped += 1
        continue

    try:
        ds = xr.open_dataset(fpath)

        # Select variables that are present in this file
        vars_present = [v for v in FORA_VARS if v in ds.data_vars]
        if not vars_present:
            print(f'  WARNING: no target variables in {fpath.name}')
            print(f'    Available: {list(ds.data_vars)[:10]}')
            ds.close()
            _n_skipped += 1
            continue

        # Clip to the CONUS analysis grid (nearest-neighbor; grids share 0.125° spacing)
        ds_clipped = ds[vars_present].sel(
            lat=CONUS_LAT, lon=CONUS_LON, method='nearest', tolerance=0.07
        )

        # Attach metadata
        ds_clipped.attrs.update({
            'source':   'NLDAS_FORA0125_M.002',
            'yyyymm':   yyyymm,
            'variables': ', '.join(vars_present),
            'grid':     '189 lat x 325 lon, 0.125 deg, clipped to OpenET extent',
            'crs':      'EPSG:4326',
        })
        for v in vars_present:
            _units = {
                'Tair':    'K',
                'Qair':    'kg/kg',
                'PSurf':   'Pa',
                'Wind_E':  'm/s',
                'Wind_N':  'm/s',
                'SWdown':  'W/m^2',
                'LWdown':  'W/m^2',
                'Rainf':   'kg/m^2',
                'PotEvap': 'kg/m^2',
            }.get(v, 'unknown')
            ds_clipped[v].attrs.update({'units': _units, 'yyyymm': yyyymm})

        ds_clipped.to_netcdf(dst_path)
        ds.close()
        _n_processed += 1

        if _n_processed % 12 == 0:
            print(f'  Processed {_n_processed} files...')

    except Exception as e:
        print(f'  ERROR {fpath.name}: {e}')
        _n_error += 1

# =============================================================================
# Step 3 — QC: verify one output file
# =============================================================================

print()
print(f'FORA processing complete:')
print(f'  Processed : {_n_processed}')
print(f'  Skipped   : {_n_skipped}')
print(f'  Errors    : {_n_error}')
print()

_proc_files = sorted(PROC_DIR.glob('FORA_*.nc'))
print(f'Processed files in {PROC_DIR}: {len(_proc_files)}')

if _proc_files:
    _qc = _proc_files[len(_proc_files) // 2]   # QC middle file
    with xr.open_dataset(_qc) as ds_qc:
        print(f'\nQC: {_qc.name}')
        print(f'  Shape: {len(ds_qc.lat)} lat x {len(ds_qc.lon)} lon')
        for v in ds_qc.data_vars:
            arr = ds_qc[v].values.flatten()
            arr = arr[np.isfinite(arr)]
            print(f'  {v:<6}  min={arr.min():.3g}  mean={arr.mean():.3g}  max={arr.max():.3g}  units={ds_qc[v].attrs.get("units","?")}')

print()
print('Next step:')
print('  Run 02_irrigation_sif_regression_conus.ipynb Section 12')
print('  to load these files, compute VPD and Wind, and run Models C and D.')
