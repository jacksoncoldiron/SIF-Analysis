#!/usr/bin/env python3
"""
Diagnostic: map RZSM coverage vs. cropland pixels.

Loads rzsm_conus_gs.nc and df_combined_gs.parquet, then produces:
  - A map showing which cropland pixels have RZSM vs. not
  - A count summary (total cropland pixels, RZSM-covered, missing, %)
  - Partial-fill check (coord mismatch vs. land-ocean mask)

Output: figures/diag_rzsm_coverage.png

Usage:
  cd /home/pielab-sandbox-jcoldiron/SIF-Analysis
  PYTHONPATH=/home/pielab-sandbox-jcoldiron/.venv/lib/python3.13/site-packages \
  /home/pielab-sandbox-jcoldiron/.venv/bin/python3 src/scripts/diag_rzsm_coverage.py
"""

from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# ── Paths ─────────────────────────────────────────────────────────────────
root     = Path(__file__).resolve().parent.parent.parent
parquet  = root / 'data' / 'processed' / 'conus' / 'regression' / 'df_combined_gs.parquet'
rzsm_nc  = root / 'data' / 'processed' / 'conus' / 'rzsm_conus_gs.nc'
fig_out  = root / 'figures' / 'diag_rzsm_coverage.png'
fig_out.parent.mkdir(parents=True, exist_ok=True)

# ── 1. Build RZSM pixel coverage map from NetCDF alone (fast) ─────────────
print('Loading rzsm_conus_gs.nc ...')
ds = xr.open_dataset(rzsm_nc)
lats = ds.coords['lat'].values          # (189,)
lons = ds.coords['lon'].values          # (325,)
rzsm_arr = ds['rzsm'].values            # (60, 189, 325)
n_times = rzsm_arr.shape[0]
ds.close()

# For each pixel: how many of 60 months have valid data?
valid_count = np.isfinite(rzsm_arr).sum(axis=0)   # (189, 325)
has_rzsm_grid = (valid_count > 0)                  # bool mask

print(f'Full NLDAS grid: {has_rzsm_grid.shape[0]}×{has_rzsm_grid.shape[1]} = '
      f'{has_rzsm_grid.size} pixels')
print(f'Pixels with ANY RZSM: {has_rzsm_grid.sum()} '
      f'({100*has_rzsm_grid.mean():.1f}% of full grid)')
print(f'Pixels fully all-months valid: {(valid_count == n_times).sum()}')
print(f'Pixels with PARTIAL coverage: {((valid_count > 0) & (valid_count < n_times)).sum()}')

# ── 2. Load unique cropland pixels from parquet (read only 2 cols) ─────────
print('\nLoading cropland pixel list from parquet ...')
df_locs = pd.read_parquet(parquet, columns=['lat', 'lon', 'rzsm'])

# Fast aggregation: per pixel, count non-null rzsm rows
# Use groupby + agg instead of apply(lambda) — much faster
print('Aggregating ...')
pix_agg = df_locs.groupby(['lat', 'lon'], sort=False).agg(
    n_valid=('rzsm', 'count'),
    n_total=('rzsm', 'size'),
).reset_index()

n_total   = len(pix_agg)
n_covered = (pix_agg['n_valid'] > 0).sum()
n_missing = n_total - n_covered
n_partial = ((pix_agg['n_valid'] > 0) & (pix_agg['n_valid'] < pix_agg['n_total'])).sum()

print(f'\nCropland pixels (parquet): {n_total:,}')
print(f'  RZSM covered:  {n_covered:,}  ({100*n_covered/n_total:.1f}%)')
print(f'  RZSM missing:  {n_missing:,}  ({100*n_missing/n_total:.1f}%)')
print(f'  Partial fill:  {n_partial}  '
      f'(> 0 means coordinate mismatch, not just land-ocean mask)')

# ── 3. Lon/lat breakdown of missing pixels ─────────────────────────────────
missing = pix_agg[pix_agg['n_valid'] == 0].copy()
print('\n--- Missing RZSM pixels by 5° longitude band ---')
missing['lon_band'] = (missing['lon'] // 5 * 5).astype(int)
print(missing.groupby('lon_band').size().rename('n_missing').to_string())

print('\n--- Missing RZSM pixels by 5° latitude band ---')
missing['lat_band'] = (missing['lat'] // 5 * 5).astype(int)
print(missing.groupby('lat_band').size().rename('n_missing').to_string())

# ── 4. Plot ────────────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Panel (a): RZSM valid fraction over the full NLDAS grid
ax = axes[0]
lon2d, lat2d = np.meshgrid(lons, lats)
frac_valid = valid_count / n_times
im = ax.pcolormesh(lon2d, lat2d, frac_valid, cmap='Blues', vmin=0, vmax=1)
fig.colorbar(im, ax=ax, shrink=0.7, label='Fraction of months with RZSM')
ax.set_title('(a) RZSM coverage — full NLDAS grid', fontsize=11)
ax.set_xlabel('Longitude'); ax.set_ylabel('Latitude')

# Panel (b): Cropland pixels coloured by RZSM presence
ax = axes[1]
covered = pix_agg[pix_agg['n_valid'] > 0]
miss    = pix_agg[pix_agg['n_valid'] == 0]
ax.scatter(covered['lon'], covered['lat'], s=0.8, c='steelblue',
           label=f'Has RZSM ({n_covered:,})', rasterized=True)
ax.scatter(miss['lon'],    miss['lat'],    s=0.8, c='tomato',
           label=f'Missing ({n_missing:,})', rasterized=True)
ax.set_title('(b) Cropland pixels: RZSM coverage', fontsize=11)
ax.set_xlabel('Longitude'); ax.set_ylabel('Latitude')
ax.legend(markerscale=4, fontsize=9)

fig.suptitle('RZSM Coverage Diagnostic', fontsize=13, y=1.01)
fig.tight_layout()
fig.savefig(fig_out, dpi=150, bbox_inches='tight')
print(f'\nFigure saved: {fig_out}')
print('\nDone.')
