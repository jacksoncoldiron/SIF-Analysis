#!/usr/bin/env python3
"""
Build spei_conus_month_180d.nc from the raw gridMET SPEI-180d NetCDF.

Why this exists
---------------
05_download_spei_multiperiod.py sources SPEI from the GRIDMET/DROUGHT
collection in Earth Engine.  Two problems with that path:
  * the existing on-disk TIFs from 19_download_drought_conus.py carry only
    8 bands and spei180d is not among them, and
  * the Earth Engine credentials in ~/.config/earthengine are expired, so a
    re-export is not currently possible without interactive re-auth.

The Climatology Lab serves the same gridMET SPEI product as a direct NetCDF
download, no authentication required:
    https://www.northwestknowledge.net/metdata/data/spei180d.nc
That file is 5-day-interval, 1/24 deg, 1980-present.  This script reduces it
to the study grid and calendar-month resolution.

Steps
-----
1. Select growing-season (Apr-Sep) pentads for 2015-2024.
2. Average pentads within each calendar month.
3. Regrid 1/24 deg (585 x 1386) -> 0.125 deg (189 x 325) by bilinear
   interpolation, matching Resampling.bilinear used in 05_ for SPEI-30d.

Output
------
data/processed/conus/spei_conus_month_180d.nc   variable: spei_180d
    dims (month: 60, lat: 189, lon: 325), coord 'yyyymm'

Then run 07_extend_panel_parquet.py to merge it into df_combined_gs.parquet.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW = PROJECT_ROOT / 'data' / 'raw' / 'gridmet_drought' / 'spei180d.nc'
# Same directory as spei_conus_month_30d.nc, which is where
# 07_extend_panel_parquet.py looks for it.
OUT_DIR = PROJECT_ROOT / 'data' / 'processed' / 'conus'
OUT = OUT_DIR / 'spei_conus_month_180d.nc'

CONUS_LON = np.arange(-124.6875, -84.0625, 0.125)   # 325
CONUS_LAT = np.arange(49.3125, 25.6875, -0.125)     # 189, N->S

YEARS = list(range(2015, 2025))
GROWING_SEASON = [4, 5, 6, 7, 8, 9]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f'Opening {RAW}')
    # No dask chunking: the script loads one month of pentads at a time
    # (~40 MB), so lazy netCDF4 indexing is sufficient.
    ds = xr.open_dataset(RAW)

    var = 'spei'
    da = ds[var]

    # gridMET lat runs N->S already; xr.interp needs monotonic coords, which
    # descending order satisfies.
    days = pd.to_datetime(da['day'].values)
    keep = np.isin(days.year, YEARS) & np.isin(days.month, GROWING_SEASON)
    print(f'Pentads selected: {int(keep.sum()):,} of {len(days):,}')

    da = da.isel(day=np.where(keep)[0])
    sel_days = pd.to_datetime(da['day'].values)
    yyyymm = np.array([d.year * 100 + d.month for d in sel_days])

    out = np.full((len(YEARS) * len(GROWING_SEASON), len(CONUS_LAT), len(CONUS_LON)),
                  np.nan, dtype='float32')
    months_out = []

    idx = 0
    for year in YEARS:
        for month in GROWING_SEASON:
            key = year * 100 + month
            months_out.append(key)
            sel = np.where(yyyymm == key)[0]
            if len(sel) == 0:
                print(f'  {key}: no pentads — left as NaN')
                idx += 1
                continue

            # Monthly mean over pentads, then regrid.
            mon = da.isel(day=sel).mean('day', skipna=True).load()
            regrid = mon.interp(
                lat=xr.DataArray(CONUS_LAT, dims='lat'),
                lon=xr.DataArray(CONUS_LON, dims='lon'),
                method='linear',
            )
            out[idx] = regrid.values.astype('float32')
            frac = np.isfinite(out[idx]).mean()
            print(f'  {key}: {len(sel)} pentads -> grid, {frac * 100:.1f}% finite')
            idx += 1

    # Structure must match spei_conus_month_30d.nc, which is what
    # 07_extend_panel_parquet.py indexes into: dims (time, lat, lon), a
    # datetime64 'time' coord, and float32 lat/lon.
    times = pd.to_datetime([f'{k // 100}-{k % 100:02d}-01' for k in months_out])

    result = xr.Dataset(
        {'spei_180d': (('time', 'lat', 'lon'), out)},
        coords={
            'time': times,
            'lat': CONUS_LAT.astype('float32'),
            'lon': CONUS_LON.astype('float32'),
        },
        attrs={
            'title': 'gridMET SPEI-180d on CONUS NLDAS grid',
            'study_years': '2015-2024',
            'source': 'gridMET SPEI-180d (Climatology Lab), '
                      'https://www.northwestknowledge.net/metdata/data/spei180d.nc',
            'processing': 'growing-season pentads averaged to calendar month; '
                          'bilinear regrid 1/24 deg -> 0.125 deg',
            'accumulation': '180 days',
            'grid': '189 lat x 325 lon, 0.125 deg',
            'crs': 'EPSG:4326',
        },
    )
    result['spei_180d'].attrs.update({'units': 'z-score', 'long_name': 'SPEI 180-day'})

    result.to_netcdf(OUT)
    print(f'\nWrote {OUT}')
    print(f'  shape: {out.shape}')
    print(f'  finite: {np.isfinite(out).mean() * 100:.1f}%')
    print(f'  range: {np.nanmin(out):.2f} to {np.nanmax(out):.2f}')
    ds.close()


if __name__ == '__main__':
    main()
