#!/usr/bin/env python3
"""
Extend df_combined_gs.parquet with new drought index columns.

Adds the following columns to the existing panel dataset:
  spei30d   — SPEI 1-month (~30d) from spei_conus_month_30d.nc
  spei60d   — SPEI 2-month (~60d) from spei_conus_month_60d.nc
  spei180d  — SPEI 6-month (~180d) from spei_conus_month_180d.nc
  rzsm      — Root zone soil moisture [kg/m²] from rzsm_conus_gs.nc
  rzsm_z    — Standardized RZSM (z-score vs. pixel-level climatology)

Skips any column that is already present in the parquet (safe to re-run).

Prerequisites:
  • 05_download_spei_multiperiod.py — must have been run first
  • 06_extract_nldas_soilm.py      — must have been run first

Usage:
  cd /home/pielab-sandbox-jcoldiron/SIF-Analysis
  PYTHONPATH=/home/pielab-sandbox-jcoldiron/.venv/lib/python3.13/site-packages \\
  /home/pielab-sandbox-jcoldiron/.venv/bin/python3 \\
    src/scripts/download/07_extend_panel_parquet.py
"""

from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr

# ─────────────────────────────────────────────────────────────────────────
# 1. Paths
# ─────────────────────────────────────────────────────────────────────────
project_root = Path(__file__).resolve().parent.parent.parent.parent
proc_dir     = project_root / 'data' / 'processed' / 'conus'
parquet_path = proc_dir / 'regression' / 'df_combined_gs.parquet'

CONUS_LON = np.arange(-124.6875, -84.0625,  0.125)
CONUS_LAT = np.arange(  49.3125,  25.6875, -0.125)

# ─────────────────────────────────────────────────────────────────────────
# 2. Load panel dataset
# ─────────────────────────────────────────────────────────────────────────
print('Loading panel:', parquet_path)
df = pd.read_parquet(parquet_path)
if 'date' not in df.columns:
    df['date'] = pd.to_datetime(df['yyyymm'], format='%Y%m')

print('Shape:', df.shape)
print('Existing columns:', df.columns.tolist())

# ─────────────────────────────────────────────────────────────────────────
# 3. Helper: read a NetCDF drought-index file → dict keyed (yyyymm, lat, lon)
# ─────────────────────────────────────────────────────────────────────────
def _nc_to_lookup(nc_path, var_name):
    """Build a DataFrame with columns [yyyymm, lat, lon, <var_name>] for merging."""
    ds = xr.open_dataset(nc_path)
    lat_vals = ds.coords['lat'].values.astype(float)
    lon_vals = ds.coords['lon'].values.astype(float)
    time_vals = ds.coords['time'].values

    rows = []
    for ti, t in enumerate(time_vals):
        ts = pd.Timestamp(t)
        yyyymm = int(ts.strftime('%Y%m'))
        arr = ds[var_name].values[ti]   # (189, 325)
        for ri, la in enumerate(lat_vals):
            for ci, lo in enumerate(lon_vals):
                v = float(arr[ri, ci])
                if np.isfinite(v):
                    rows.append((yyyymm, round(la, 4), round(lo, 4), v))

    ds.close()
    return pd.DataFrame(rows, columns=['yyyymm', 'lat', 'lon', var_name])


# ─────────────────────────────────────────────────────────────────────────
# 4. Efficient merge: use xarray + lat/lon lookup (much faster than row-by-row)
# ─────────────────────────────────────────────────────────────────────────
def _add_index_column(df_panel, nc_path, var_name, col_name):
    """Merge a NetCDF field into df_panel as a new column.

    Uses xarray.sel for fast lat/lon lookup rather than iterating rows.
    """
    if col_name in df_panel.columns:
        print('  {} already present — skipping.'.format(col_name))
        return df_panel

    if not nc_path.exists():
        print('  WARNING: {} not found — run the download script first.'.format(nc_path.name))
        print('    Expected:', nc_path)
        return df_panel

    print('  Adding {} from {} ...'.format(col_name, nc_path.name))
    ds = xr.open_dataset(nc_path)

    # Build yyyymm → DataArray mapping
    time_vals = pd.DatetimeIndex(ds.coords['time'].values)
    ym_to_idx = {int(t.strftime('%Y%m')): i for i, t in enumerate(time_vals)}

    lat_arr  = ds.coords['lat'].values.astype(float)
    lon_arr  = ds.coords['lon'].values.astype(float)
    lat_to_r = {round(v, 4): i for i, v in enumerate(lat_arr)}
    lon_to_c = {round(v, 4): i for i, v in enumerate(lon_arr)}

    def _lookup(row):
        ti = ym_to_idx.get(int(row['yyyymm']))
        ri = lat_to_r.get(round(float(row['lat']), 4))
        ci = lon_to_c.get(round(float(row['lon']), 4))
        if ti is None or ri is None or ci is None:
            return np.nan
        v = float(ds[var_name].values[ti, ri, ci])
        return v if np.isfinite(v) else np.nan

    # Vectorized per unique (yyyymm, lat, lon) combination
    keys = df_panel[['yyyymm', 'lat', 'lon']].drop_duplicates()
    keys[col_name] = keys.apply(_lookup, axis=1)
    ds.close()

    df_panel = df_panel.merge(
        keys[['yyyymm', 'lat', 'lon', col_name]],
        on=['yyyymm', 'lat', 'lon'], how='left',
    )
    n_valid = df_panel[col_name].notna().sum()
    print('    {:,} / {:,} rows filled ({:.1f}%)'.format(
        n_valid, len(df_panel), 100 * n_valid / len(df_panel)))
    return df_panel


# ─────────────────────────────────────────────────────────────────────────
# 5. Add SPEI columns
# ─────────────────────────────────────────────────────────────────────────
SPEI_PERIODS = [
    ('30d',  'spei_30d',  'spei30d'),
    ('60d',  'spei_60d',  'spei60d'),
    ('180d', 'spei_180d', 'spei180d'),
]

for period_label, var_name, col_name in SPEI_PERIODS:
    nc_path = proc_dir / 'spei_conus_month_{}.nc'.format(period_label)
    df = _add_index_column(df, nc_path, var_name, col_name)

# ─────────────────────────────────────────────────────────────────────────
# 6. Add RZSM and standardized RZSM
# ─────────────────────────────────────────────────────────────────────────
rzsm_path = proc_dir / 'rzsm_conus_gs.nc'
df = _add_index_column(df, rzsm_path, 'rzsm', 'rzsm')

# Standardize RZSM: z-score relative to each pixel's own seasonal climatology
# (so negative z = drier than normal for that pixel × month combination)
if 'rzsm' in df.columns and 'rzsm_z' not in df.columns and df['rzsm'].notna().sum() > 0:
    print('  Computing rzsm_z (pixel × month climatology z-score)...')
    df_r = df[df['rzsm'].notna()].copy()
    # Extract month from yyyymm
    df_r['month'] = df_r['yyyymm'].astype(str).str[4:6].astype(int)
    _clim = (df_r.groupby(['lat', 'lon', 'month'])['rzsm']
             .agg(['mean', 'std'])
             .reset_index()
             .rename(columns={'mean': '_rzsm_mean', 'std': '_rzsm_std'}))
    df_r = df_r.merge(_clim, on=['lat', 'lon', 'month'], how='left')
    df_r['rzsm_z'] = (df_r['rzsm'] - df_r['_rzsm_mean']) / df_r['_rzsm_std'].replace(0, np.nan)
    df = df.merge(df_r[['yyyymm', 'lat', 'lon', 'rzsm_z']], on=['yyyymm', 'lat', 'lon'],
                  how='left')
    print('    rzsm_z: {:,} non-null'.format(df['rzsm_z'].notna().sum()))

# ─────────────────────────────────────────────────────────────────────────
# 7. Save extended parquet
# ─────────────────────────────────────────────────────────────────────────
print('\nFinal columns:', df.columns.tolist())
print('Final shape:  ', df.shape)

df.to_parquet(parquet_path, index=False)
print('\nSaved to:', parquet_path)
print('\nDone. You can now run the visualization notebook to generate')
print('Figure 2 and Figure 3 variants for all drought indices.')
