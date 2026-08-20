#!/usr/bin/env python3
"""
Extract Root Zone Soil Moisture (RZSM) from raw NLDAS-2 Noah LSM files.

Reads the raw NLDAS_NOAH0125_M.A{YYYYMM}.020.nc files already on disk,
extracts the soil moisture variable for the full column (SoilM_0_100cm or
RootMoist), and saves one processed NetCDF per growing-season month.

Source:  data/raw/nldas/NLDAS_NOAH0125_M.A{YYYYMM}.020.nc
Output:  data/processed/conus/nldas/NLDAS_SoilM_{YYYYMM}.nc
         data/processed/conus/rzsm_conus_gs.nc   (stacked growing-season file)

Usage:
  cd /home/pielab-sandbox-jcoldiron/SIF-Analysis
  PYTHONPATH=/home/pielab-sandbox-jcoldiron/.venv/lib/python3.13/site-packages \\
  /home/pielab-sandbox-jcoldiron/.venv/bin/python3 \\
    src/scripts/download/06_extract_nldas_soilm.py
"""

from pathlib import Path
import numpy as np
import xarray as xr

# ─────────────────────────────────────────────────────────────────────────
# 1. Paths
# ─────────────────────────────────────────────────────────────────────────
project_root  = Path(__file__).resolve().parent.parent.parent.parent
raw_nldas_dir = project_root / 'data' / 'raw' / 'nldas'
proc_nldas    = project_root / 'data' / 'processed' / 'conus' / 'nldas'
proc_dir      = project_root / 'data' / 'processed' / 'conus'

proc_nldas.mkdir(parents=True, exist_ok=True)

YEARS          = list(range(2015, 2025))
GROWING_SEASON = [4, 5, 6, 7, 8, 9]   # April–September

# CONUS grid — must match other processed files
CONUS_LON = np.arange(-124.6875, -84.0625,  0.125)
CONUS_LAT = np.arange(  49.3125,  25.6875, -0.125)

# ─────────────────────────────────────────────────────────────────────────
# 2. Variable priority: try each name in order until one is found
# ─────────────────────────────────────────────────────────────────────────
SOILM_CANDIDATES = [
    'SoilM_0_100cm',   # total column 0–100 cm
    'RootMoist',       # root zone moisture (0–100 cm in NLDAS Noah)
    'SoilM_0_10cm',    # fallback: 0–10 cm only
]

# ─────────────────────────────────────────────────────────────────────────
# 3. Scan one file to identify the correct variable name
# ─────────────────────────────────────────────────────────────────────────
_sample_files = sorted(raw_nldas_dir.glob('NLDAS_NOAH0125_M.A20190*.020.nc'))
if not _sample_files:
    raise FileNotFoundError(
        'No raw NLDAS Noah files found in: ' + str(raw_nldas_dir) +
        '\nExpected pattern: NLDAS_NOAH0125_M.A{YYYYMM}.020.nc'
    )

_ds_sample = xr.open_dataset(_sample_files[0])
print('All variables in sample NLDAS Noah file:')
for v in _ds_sample.data_vars:
    print(' ', v, _ds_sample[v].dims, _ds_sample[v].attrs.get('units', ''))

_soilm_var = None
for candidate in SOILM_CANDIDATES:
    if candidate in _ds_sample.data_vars:
        _soilm_var = candidate
        break

if _soilm_var is None:
    _ds_sample.close()
    raise ValueError(
        'None of the expected soil moisture variables found.\n'
        'Available variables: ' + str(list(_ds_sample.data_vars)) + '\n'
        'Update SOILM_CANDIDATES in this script.'
    )

print('\nUsing soil moisture variable: {}'.format(_soilm_var))
print('Units:', _ds_sample[_soilm_var].attrs.get('units', 'unknown'))
_ds_sample.close()

# ─────────────────────────────────────────────────────────────────────────
# 4. Extract growing-season months
# ─────────────────────────────────────────────────────────────────────────
out_records = []   # list of (yyyymm, array_2d)
n_missing = 0

for year in YEARS:
    for month in GROWING_SEASON:
        yyyymm = '{:04d}{:02d}'.format(year, month)
        raw_fp = raw_nldas_dir / 'NLDAS_NOAH0125_M.A{}.020.nc'.format(yyyymm)
        out_fp = proc_nldas / 'NLDAS_SoilM_{}.nc'.format(yyyymm)

        if out_fp.exists():
            # Load cached processed file
            ds_c = xr.open_dataset(out_fp)
            arr  = ds_c['rzsm'].values.squeeze().astype(np.float32)
            ds_c.close()
            out_records.append((yyyymm, arr))
            continue

        if not raw_fp.exists():
            print('  MISSING raw file:', raw_fp.name)
            n_missing += 1
            continue

        ds = xr.open_dataset(raw_fp)
        arr = ds[_soilm_var].values
        ds.close()

        if arr.ndim == 3:
            arr = arr[0]   # drop time dim if present
        arr = arr.astype(np.float32)

        # Subset from the full NLDAS CONUS grid (224×464) to the study domain (189×325)
        # using xarray coordinate selection.
        if arr.shape != (len(CONUS_LAT), len(CONUS_LON)):
            # Re-open as xarray to do coordinate-based subsetting
            ds2 = xr.open_dataset(raw_fp)
            # Attempt coordinate names lat/lon or g4_lat_0/g4_lon_1
            lat_coord = 'lat' if 'lat' in ds2.coords else (
                        'g4_lat_0' if 'g4_lat_0' in ds2.coords else None)
            lon_coord = 'lon' if 'lon' in ds2.coords else (
                        'g4_lon_1' if 'g4_lon_1' in ds2.coords else None)
            if lat_coord is None or lon_coord is None:
                ds2.close()
                print('  Cannot identify lat/lon coords for {} — skipping'.format(yyyymm))
                n_missing += 1
                continue
            da = ds2[_soilm_var]
            if 'time' in da.dims:
                da = da.isel(time=0)
            # Nearest-neighbour selection for each study-domain coordinate
            arr_sub = da.sel(
                {lat_coord: CONUS_LAT, lon_coord: CONUS_LON},
                method='nearest',
            ).values.astype(np.float32)
            ds2.close()
            if arr_sub.shape != (len(CONUS_LAT), len(CONUS_LON)):
                print('  Still shape mismatch after subset for {} ({}) — skipping'.format(
                    yyyymm, arr_sub.shape))
                n_missing += 1
                continue
            arr = arr_sub

        # Replace fill values with NaN
        arr = np.where((arr < 0) | (arr > 2000), np.nan, arr)

        # Save individual processed file
        ds_out = xr.Dataset(
            {'rzsm': (['lat', 'lon'], arr,
                      {'long_name': 'Root zone soil moisture ({})'.format(_soilm_var),
                       'source_var': _soilm_var,
                       'units': 'kg m-2'})},
            coords={
                'lat': (['lat'], CONUS_LAT.astype(np.float32)),
                'lon': (['lon'], CONUS_LON.astype(np.float32)),
            },
        )
        ds_out.to_netcdf(out_fp, encoding={'rzsm': {'zlib': True, 'complevel': 4}})
        ds_out.close()

        out_records.append((yyyymm, arr))
        print('  Processed: {} ({})'.format(yyyymm, _soilm_var))

print('\nExtracted {:d} growing-season months  |  {:d} missing'.format(
    len(out_records), n_missing))

# ─────────────────────────────────────────────────────────────────────────
# 5. Stack into a single growing-season NetCDF for easy loading
# ─────────────────────────────────────────────────────────────────────────
import pandas as pd

stacked_path = proc_dir / 'rzsm_conus_gs.nc'
if out_records:
    times  = [pd.Timestamp(r[0][:4] + '-' + r[0][4:] + '-01') for r in out_records]
    stack  = np.stack([r[1] for r in out_records], axis=0)   # (n_gs, 189, 325)

    ds_stack = xr.Dataset(
        {'rzsm': (['time', 'lat', 'lon'], stack.astype(np.float32),
                  {'long_name': 'Root zone soil moisture', 'source_var': _soilm_var,
                   'units': 'kg m-2'})},
        coords={
            'time': times,
            'lat':  (['lat'], CONUS_LAT.astype(np.float32)),
            'lon':  (['lon'], CONUS_LON.astype(np.float32)),
        },
    )
    ds_stack.attrs.update({
        'title':       'NLDAS-2 Noah RZSM — growing season months 2015-2024',
        'source_var':  _soilm_var,
        'grid':        '189 lat x 325 lon, 0.125 deg, EPSG:4326',
    })
    enc = {'rzsm': {'zlib': True, 'complevel': 4, 'dtype': 'float32'}}
    ds_stack.to_netcdf(stacked_path, encoding=enc)
    ds_stack.close()
    print('Stacked RZSM file saved:', stacked_path)
    print('Shape: {} x 189 x 325'.format(stack.shape[0]))
else:
    print('No records to stack — check for missing raw files.')

print('\nDone. Next step: run 07_extend_panel_parquet.py to add rzsm to the panel.')
