#!/usr/bin/env python3
"""
Pixel -> state lookup, US-boundary clip, and fixed-effects regressions.

Two things this script establishes:

1. A pixel -> state lookup for every (lat, lon) in df_combined_gs.parquet,
   written to data/processed/conus/regression/pixel_states.parquet.
   Columns: lat, lon, state, in_conus

   This doubles as the US-boundary clip for the REGRESSION sample.  The
   existing clip (_boundary_mask in the 03_ notebook) is applied only when
   drawing maps; the pooled regression in 02_ never had it applied.  Of the
   25,153 pixels in the panel, only ~8,800 fall inside a lower-48 state
   polygon.  The rest are ocean / Mexico / Canada cells that survived the
   CDL cropland-fraction threshold because their NLDAS grid cell straddles
   the border (Known Data Quality Issue #3).  They are ~98% empty, but the
   ~2% that carry values enter the regression.

2. Fixed-effects specifications controlling for location, replacing the
   pooled OLS.  See MODELS below.

Run:
    /home/jcoldiron/miniforge3/envs/sif/bin/python \
        src/scripts/08_build_pixel_states_and_fe.py
"""

from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
import statsmodels.api as sm
from shapely.geometry import Point

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REG_DIR = PROJECT_ROOT / 'data' / 'processed' / 'conus' / 'regression'
PANEL = REG_DIR / 'df_combined_gs.parquet'
OUT_STATES = REG_DIR / 'pixel_states.parquet'

# ne_10m is the finest Natural Earth admin-1 tier.  Resolution matters here
# because the pixels in question sit ON the coastline / national border.
NE_STATES_URL = (
    'https://github.com/nvkelso/natural-earth-vector/raw/master/'
    'geojson/ne_10m_admin_1_states_provinces.geojson'
)

# 0.125 deg NLDAS cell is ~13.9 km at mid-latitudes; a pixel CENTRE that falls
# just outside the polygon can still be a legitimate coastal cropland cell.
# Buffer the dissolved US polygon by half a cell so those are retained.
HALF_CELL_DEG = 0.0625


def build_pixel_states() -> pd.DataFrame:
    df = pd.read_parquet(PANEL, columns=['lat', 'lon'])
    px = df.drop_duplicates().reset_index(drop=True)
    print(f'Panel pixels: {len(px):,}')

    st = gpd.read_file(NE_STATES_URL)
    us = st[
        (st['admin'] == 'United States of America')
        & (~st['name'].isin(['Alaska', 'Hawaii']))
    ][['name', 'geometry']].copy()
    print(f'Lower-48 state polygons: {len(us)}')

    pts = gpd.GeoDataFrame(
        px,
        geometry=[Point(x, y) for x, y in zip(px['lon'], px['lat'])],
        crs='EPSG:4326',
    )

    # Exact within-state assignment (used for the state label).
    joined = gpd.sjoin(pts, us, how='left', predicate='within')
    joined = joined.drop_duplicates(subset=['lat', 'lon'])
    px['state'] = joined['name'].values

    # Half-cell-buffered CONUS membership (used for the analysis clip), so a
    # coastal cell whose centre falls just offshore is not discarded.
    us_union = us.geometry.union_all()
    us_buffered = us_union.buffer(HALF_CELL_DEG)
    px['in_conus'] = [us_buffered.contains(g) for g in pts.geometry]

    # Give buffered-in pixels that missed exact assignment their nearest state.
    need = px['state'].isna() & px['in_conus']
    if need.any():
        us_sind = us.sindex
        for i in np.where(need.values)[0]:
            p = Point(px.at[i, 'lon'], px.at[i, 'lat'])
            cand = list(us_sind.nearest(p, return_all=False)[1])
            if cand:
                px.at[i, 'state'] = us.iloc[cand[0]]['name']

    print(f"  exact in-state      : {joined['name'].notna().sum():,}")
    print(f"  in_conus (buffered) : {int(px['in_conus'].sum()):,}")
    print(f"  clipped out         : {int((~px['in_conus']).sum()):,}")
    return px


def _fit_ols(y, X, label, cluster=None, n_absorbed=0):
    model = sm.OLS(y, X)
    if cluster is not None:
        res = model.fit(cov_type='cluster', cov_kwds={'groups': cluster})
    else:
        res = model.fit(cov_type='HC3')
    print(f'\n--- {label} ---')
    print(f'N = {int(res.nobs):,}   R2 = {res.rsquared:.4f}')
    if n_absorbed:
        # R2 here is on the demeaned outcome (within R2).
        print(f'   (within R2; {n_absorbed:,} pixel effects absorbed)')
    for term in ['spei90d', 'delta_et', 'spei_x_det']:
        if term in res.params.index:
            b, se, p = res.params[term], res.bse[term], res.pvalues[term]
            stars = '***' if p < .001 else '**' if p < .01 else '*' if p < .05 else ''
            print(f'   {term:12s} b = {b: .6f}   SE = {se:.6f}   p = {p:.3g} {stars}')
    return res


def main():
    px = build_pixel_states()
    px.to_parquet(OUT_STATES, index=False)
    print(f'\nWrote {OUT_STATES}')

    df = pd.read_parquet(PANEL)
    df = df.merge(px[['lat', 'lon', 'state', 'in_conus']], on=['lat', 'lon'], how='left')
    df['spei_x_det'] = df['spei90d'] * df['delta_et']

    cc = df.dropna(subset=['sif_z', 'delta_et', 'spei90d']).copy()
    print(f'\nComplete cases (as published): {len(cc):,}')
    print(f'  of which outside CONUS      : {int((~cc["in_conus"]).sum()):,} '
          f'({100 * (~cc["in_conus"]).mean():.1f}%)')

    month_d = pd.get_dummies(cc['month'], prefix='m', drop_first=True).astype(float)

    # Model A — pooled OLS, as currently published (no boundary clip).
    XA = pd.concat(
        [cc[['spei90d', 'delta_et', 'spei_x_det']].astype(float), month_d], axis=1
    )
    XA = sm.add_constant(XA)
    _fit_ols(cc['sif_z'].astype(float), XA, 'Model A: pooled OLS, NO clip (published)')

    # Model B — same specification, clipped to CONUS.
    ccb = cc[cc['in_conus']].copy()
    month_b = pd.get_dummies(ccb['month'], prefix='m', drop_first=True).astype(float)
    XB = pd.concat(
        [ccb[['spei90d', 'delta_et', 'spei_x_det']].astype(float), month_b], axis=1
    )
    XB = sm.add_constant(XB)
    _fit_ols(ccb['sif_z'].astype(float), XB, 'Model B: pooled OLS, CLIPPED to CONUS')

    # Model C — pixel + year + month FE, SE clustered by pixel.
    # Pixel FE absorbed by within-transformation (demeaning) rather than
    # ~8.8k dummy columns; year and month enter as dummies.
    d = ccb.copy()
    d['pix'] = d['lat'].round(4).astype(str) + '_' + d['lon'].round(4).astype(str)

    terms = ['sif_z', 'spei90d', 'delta_et', 'spei_x_det']
    year_d = pd.get_dummies(d['year'], prefix='y', drop_first=True).astype(float)
    month_c = pd.get_dummies(d['month'], prefix='m', drop_first=True).astype(float)

    work = pd.concat([d[terms].astype(float), year_d, month_c], axis=1)
    cols = list(work.columns)
    work['pix'] = d['pix'].values

    # Within-transform every column on the pixel key.
    grouped = work.groupby('pix', sort=False)
    demeaned = work[cols] - grouped[cols].transform('mean')

    n_pix = d['pix'].nunique()
    yC = demeaned['sif_z']
    XC = demeaned[[c for c in cols if c != 'sif_z']]
    _fit_ols(yC, XC, 'Model C: pixel + year + month FE, clustered by pixel',
             cluster=d['pix'].values, n_absorbed=n_pix)

    print('\nDone.')


if __name__ == '__main__':
    main()
