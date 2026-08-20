#!/usr/bin/env python3
"""
Extract SPEI-30d and SPEI-180d drought indices and save as CONUS NetCDF files.

Source for SPEI-30d:
  Already on disk in GRIDMET/DROUGHT monthly TIFs (downloaded by
  19_download_drought_conus.py).  Band order:
    1=spi30d  2=spi90d  3=spei30d  4=spei90d  5=eddi30d  6=eddi90d  7=pdsi  8=z
  This script reads band 3 (spei30d) from each monthly TIF.

Source for SPEI-180d:
  Google Earth Engine, GRIDMET/DROUGHT collection, band 'spei180d'.
  Requires a valid GEE credentials file (~/.config/earthengine/credentials).
  Set DOWNLOAD_SPEI180 = False to skip if GEE is not available.

Output files (matching the format expected by 07_extend_panel_parquet.py):
  data/processed/conus/spei_conus_month_30d.nc   — variable: spei_30d
  data/processed/conus/spei_conus_month_180d.nc  — variable: spei_180d

Note: SPEI-60d is not available in GRIDMET/DROUGHT (no 60-day accumulation).
      SPEI-90d is already in df_combined_gs.parquet as 'spei90d'.

Usage:
  cd /home/pielab-sandbox-jcoldiron/SIF-Analysis
  PYTHONPATH=/home/pielab-sandbox-jcoldiron/.venv/lib/python3.13/site-packages \\
  /home/pielab-sandbox-jcoldiron/.venv/bin/python3 \\
    src/scripts/download/05_download_spei_multiperiod.py
"""

from pathlib import Path
import numpy as np
import xarray as xr
import rasterio
from rasterio.warp import reproject, Resampling
from rasterio.transform import from_bounds
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────
# 1. Configuration
# ─────────────────────────────────────────────────────────────────────────
project_root   = Path(__file__).resolve().parent.parent.parent.parent
gridmet_dir    = project_root / 'data' / 'raw' / 'drought_gridmet'
proc_dir       = project_root / 'data' / 'processed' / 'conus'
proc_dir.mkdir(parents=True, exist_ok=True)

# The NLDAS 0.125° CONUS analysis grid (must match all other processed files)
CONUS_LON = np.arange(-124.6875, -84.0625,  0.125)   # 325 longitudes
CONUS_LAT = np.arange(  49.3125,  25.6875, -0.125)   # 189 latitudes (N→S)
N_LAT, N_LON = len(CONUS_LAT), len(CONUS_LON)

YEARS          = list(range(2015, 2025))
GROWING_SEASON = [4, 5, 6, 7, 8, 9]   # April–September

# Band index (1-based rasterio) for each SPEI period in the GRIDMET TIFs.
# Order in downloaded TIFs: spi30d, spi90d, SPEI30d, spei90d, eddi30d, eddi90d, pdsi, z
GRIDMET_BAND_SPEI30 = 3   # 1-indexed

# Set False to skip GEE re-download for SPEI-180d (e.g. if GEE credentials absent)
DOWNLOAD_SPEI180 = True

print('Project root:', project_root)
print('GRIDMET dir: ', gridmet_dir)
print('Output dir:  ', proc_dir)
print(f'CONUS grid:   {N_LAT} lat × {N_LON} lon')

# ─────────────────────────────────────────────────────────────────────────
# 2. Helper: reproject a 2-D array from source rasterio dataset → CONUS grid
# ─────────────────────────────────────────────────────────────────────────
_CONUS_TRANSFORM = from_bounds(
    west  = float(CONUS_LON[0])  - 0.0625,
    east  = float(CONUS_LON[-1]) + 0.0625,
    south = float(CONUS_LAT[-1]) - 0.0625,
    north = float(CONUS_LAT[0])  + 0.0625,
    width  = N_LON,
    height = N_LAT,
)

def _to_conus_grid(src_arr, src_transform, src_crs):
    """Reproject a 2-D float32 array into the NLDAS CONUS grid."""
    dst = np.full((N_LAT, N_LON), np.nan, dtype=np.float32)
    reproject(
        source      = src_arr.astype(np.float32),
        destination = dst,
        src_transform  = src_transform,
        src_crs        = src_crs,
        dst_transform  = _CONUS_TRANSFORM,
        dst_crs        = 'EPSG:4326',
        resampling     = Resampling.bilinear,
        src_nodata     = np.nan,
        dst_nodata     = np.nan,
    )
    return dst


# ─────────────────────────────────────────────────────────────────────────
# 3. Extract SPEI-30d from existing GRIDMET drought TIFs
# ─────────────────────────────────────────────────────────────────────────
out_30d = proc_dir / 'spei_conus_month_30d.nc'

if out_30d.exists():
    print('\nspei_conus_month_30d.nc already exists — skipping extraction.')
else:
    print('\n── Extracting SPEI-30d from GRIDMET drought TIFs ──')
    tif_files = sorted(gridmet_dir.glob('GRIDMET_drought_*.tif'))
    print(f'  Found {len(tif_files)} GRIDMET drought TIF files')

    if not tif_files:
        print('  ERROR: no GRIDMET_drought_*.tif files found in:', gridmet_dir)
        print('  Run 19_download_drought_conus.py first.')
    else:
        records_30d = []   # (timestamp, 2D array)

        for fp in sorted(tif_files):
            # Parse YYYYMM from filename
            stem = fp.stem  # e.g. 'GRIDMET_drought_201504'
            yyyymm_str = stem.split('_')[-1]   # '201504'
            try:
                year  = int(yyyymm_str[:4])
                month = int(yyyymm_str[4:6])
            except Exception:
                continue

            if year not in YEARS:
                continue
            if month not in GROWING_SEASON:
                continue

            with rasterio.open(fp) as src:
                n_bands = src.count
                if n_bands < GRIDMET_BAND_SPEI30:
                    print(f'  WARNING {fp.name}: only {n_bands} bands, expected ≥3 — skipping')
                    continue

                arr = src.read(GRIDMET_BAND_SPEI30).astype(np.float32)
                # Replace fill/nodata with NaN
                nd = src.nodata
                if nd is not None:
                    arr[arr == nd] = np.nan
                arr[~np.isfinite(arr)] = np.nan
                # Values outside SPEI range ±5 are clearly fill values
                arr = np.where(np.abs(arr) > 5, np.nan, arr)

                # Check if TIF is already on the CONUS grid or needs reprojection
                if arr.shape == (N_LAT, N_LON):
                    conus_arr = arr
                else:
                    conus_arr = _to_conus_grid(arr, src.transform, src.crs)
                    print(f'    Reprojected {fp.name}: {arr.shape} → {conus_arr.shape}')

            ts = pd.Timestamp(f'{year}-{month:02d}-01')
            records_30d.append((ts, conus_arr))
            if len(records_30d) % 10 == 0:
                print(f'  Processed {len(records_30d)} months...')

        print(f'  Extracted {len(records_30d)} growing-season months of SPEI-30d')

        if records_30d:
            records_30d.sort(key=lambda x: x[0])
            times  = [r[0] for r in records_30d]
            stack  = np.stack([r[1] for r in records_30d], axis=0)  # (n, 189, 325)

            ds_30d = xr.Dataset(
                {'spei_30d': (['time', 'lat', 'lon'], stack,
                              {'units': 'dimensionless',
                               'long_name': 'SPEI 30d (1-month accumulation)',
                               'source': 'GRIDMET/DROUGHT via GEE, band spei30d'})},
                coords={
                    'time': times,
                    'lat':  (['lat'], CONUS_LAT.astype(np.float32)),
                    'lon':  (['lon'], CONUS_LON.astype(np.float32)),
                },
            )
            ds_30d.attrs.update({
                'title':       'GRIDMET SPEI-30d on CONUS NLDAS grid',
                'source':      'GRIDMET/DROUGHT, spei30d band, extracted from monthly TIFs',
                'study_years': '2015-2024',
                'grid':        f'{N_LAT} lat x {N_LON} lon, 0.125 deg, EPSG:4326',
            })
            enc = {'spei_30d': {'zlib': True, 'complevel': 4, 'dtype': 'float32'}}
            ds_30d.to_netcdf(out_30d, encoding=enc)
            ds_30d.close()
            print(f'  Saved: {out_30d}')
            print(f'  Shape: {stack.shape[0]} time steps × {N_LAT} lat × {N_LON} lon')
        else:
            print('  No records extracted — check GRIDMET drought TIF directory.')


# ─────────────────────────────────────────────────────────────────────────
# 4. Extract SPEI-180d from GEE (GRIDMET/DROUGHT band spei180d)
# ─────────────────────────────────────────────────────────────────────────
out_180d = proc_dir / 'spei_conus_month_180d.nc'

if out_180d.exists():
    print('\nspei_conus_month_180d.nc already exists — skipping.')
elif not DOWNLOAD_SPEI180:
    print('\nSPEI-180d download skipped (DOWNLOAD_SPEI180=False).')
else:
    print('\n── Downloading SPEI-180d from GEE (GRIDMET/DROUGHT) ──')
    try:
        import ee
    except ImportError:
        print('  ERROR: earthengine-api not installed.')
        print('  Install with: pip install earthengine-api')
        print('  Then re-run this script.')
        DOWNLOAD_SPEI180 = False

    if DOWNLOAD_SPEI180:
        try:
            ee.Initialize(project='et-research-489120')
            print('  GEE initialized.')
        except Exception as e:
            print(f'  ERROR: GEE initialization failed: {e}')
            print('  Run: python3 -c "import ee; ee.Authenticate(auth_mode=\'notebook\')"')
            DOWNLOAD_SPEI180 = False

    if DOWNLOAD_SPEI180:
        # CONUS NLDAS grid parameters for GEE computePixels
        # Grid: 325 wide × 189 tall, 0.125° cells, starting at (-124.75°, 49.375°) top-left
        GEE_TRANSFORM = {
            'scaleX':     0.125,
            'shearX':     0.0,
            'translateX': float(CONUS_LON[0]) - 0.0625,   # left edge = -124.75
            'shearY':     0.0,
            'scaleY':    -0.125,
            'translateY': float(CONUS_LAT[0]) + 0.0625,   # top  edge =  49.375
        }
        GEE_DIMS = {'width': N_LON, 'height': N_LAT}

        import datetime
        import time as _time

        gridmet_coll = ee.ImageCollection('GRIDMET/DROUGHT')
        records_180d = []
        failed = []

        for year in YEARS:
            for month in GROWING_SEASON:
                ts      = pd.Timestamp(f'{year}-{month:02d}-01')
                start   = ts.strftime('%Y-%m-%d')
                if month == 12:
                    end_excl = f'{year+1}-01-01'
                else:
                    end_excl = f'{year}-{month+1:02d}-01'

                try:
                    img = (gridmet_coll
                           .filterDate(start, end_excl)
                           .select(['spei180d'])
                           .mean())
                    raw = ee.data.computePixels({
                        'expression': img,
                        'fileFormat': 'GEO_TIFF',
                        'grid': {
                            'crsCode':        'EPSG:4326',
                            'affineTransform': GEE_TRANSFORM,
                            'dimensions':      GEE_DIMS,
                        },
                    })
                    import io
                    with rasterio.open(io.BytesIO(raw)) as src:
                        arr = src.read(1).astype(np.float32)
                        nd  = src.nodata
                        if nd is not None:
                            arr[arr == nd] = np.nan
                    arr[np.abs(arr) > 5] = np.nan
                    records_180d.append((ts, arr))
                    print(f'  {year}-{month:02d}: ok  (valid={np.isfinite(arr).sum()})')
                    _time.sleep(0.2)   # gentle rate-limit

                except Exception as e:
                    print(f'  {year}-{month:02d}: FAILED — {e}')
                    failed.append((year, month))
                    _time.sleep(1)

        print(f'\n  Downloaded {len(records_180d)} months, {len(failed)} failed.')

        if records_180d:
            records_180d.sort(key=lambda x: x[0])
            times  = [r[0] for r in records_180d]
            stack  = np.stack([r[1] for r in records_180d], axis=0)

            ds_180d = xr.Dataset(
                {'spei_180d': (['time', 'lat', 'lon'], stack,
                               {'units': 'dimensionless',
                                'long_name': 'SPEI 180d (6-month accumulation)',
                                'source': 'GRIDMET/DROUGHT via GEE, band spei180d'})},
                coords={
                    'time': times,
                    'lat':  (['lat'], CONUS_LAT.astype(np.float32)),
                    'lon':  (['lon'], CONUS_LON.astype(np.float32)),
                },
            )
            ds_180d.attrs.update({
                'title':       'GRIDMET SPEI-180d on CONUS NLDAS grid',
                'source':      'GRIDMET/DROUGHT, spei180d band, via GEE computePixels',
                'study_years': '2015-2024',
                'grid':        f'{N_LAT} lat x {N_LON} lon, 0.125 deg, EPSG:4326',
            })
            enc = {'spei_180d': {'zlib': True, 'complevel': 4, 'dtype': 'float32'}}
            ds_180d.to_netcdf(out_180d, encoding=enc)
            ds_180d.close()
            print(f'  Saved: {out_180d}')
        else:
            print('  No SPEI-180d records — GEE download failed for all months.')


# ─────────────────────────────────────────────────────────────────────────
# 5. Summary
# ─────────────────────────────────────────────────────────────────────────
print('\n' + '='*60)
print('SPEI extraction complete.')
print('  spei_conus_month_30d.nc  :', 'EXISTS' if out_30d.exists()  else 'MISSING')
print('  spei_conus_month_180d.nc :', 'EXISTS' if out_180d.exists() else 'MISSING')
print('\nNote: SPEI-60d is not available in GRIDMET/DROUGHT (no 60-day band).')
print('      SPEI-90d is already in df_combined_gs.parquet as spei90d.')
print('\nNext step: run 06_extract_nldas_soilm.py, then 07_extend_panel_parquet.py')
