#!/usr/bin/env python3
"""
Figure overhaul patch, round 3 (2026-08-20).

Applies the change list to 03_visualizations_conus.ipynb:

  * All maps  — study boundary snapped to WHOLE state polygons (no straight
                meridian cut); tighter zoom so CA sits flush at the left edge.
                The analysis sample is clipped to the same state set, which
                also removes the out-of-CONUS pixels that were leaking into
                the regressions (Known Data Quality Issue #3).
  * fig01a    — unchanged styling + new heatmap0 colour variant.
  * fig01b    — unchanged styling + new heatmap0 colour variant.
  * fig01c    — larger axis/legend text, alpha-based density, CA + Iowa
                least-squares fit lines in matching colours.
  * fig01d    — NEW: first-year (2015) mean growing-season Human ET vs trend.
  * fig02     — boundary/zoom formatting + colourbar text matched to fig01.
  * fig02b    — NEW: 3x3 bivariate maps, per-pixel slope x mean drought.
  * fig03     — legend moved inside (lower right); CA-only and Iowa-only
                case-study variants.
  * Section 8 — NEW: fixed-effects regressions controlling for location.

Run:
    /home/jcoldiron/miniforge3/envs/sif/bin/python \
        src/scripts/patch_viz_notebook_v3.py
"""

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
NB = PROJECT_ROOT / 'src' / 'notebooks' / 'conus' / '03_visualizations_conus.ipynb'


# =============================================================================
# Cell sources
# =============================================================================

CELL_BASEMAP = r'''
import geopandas as gpd
from pyproj import Transformer
from shapely.geometry import Point, Polygon as _SPoly

# ── EPSG:4326 → EPSG:5070 (Albers Equal Area Conic, NAD83) ───────────────
_t5070 = Transformer.from_crs('EPSG:4326', 'EPSG:5070', always_xy=True)

# ── Projected centre coordinates for pcolormesh (shading='nearest') ───────
_LON_GRID, _LAT_GRID = np.meshgrid(CONUS_LON, CONUS_LAT)   # (189, 325)
_X5070, _Y5070 = _t5070.transform(_LON_GRID, _LAT_GRID)    # (189, 325) metres

# ── Natural Earth boundaries ──────────────────────────────────────────────
# 50m tier, not 110m: at 110m the state polygons are too generalised to
# assign 0.125° pixels reliably (coastal and border cells land outside every
# polygon), which is what produced the border artifacts.
_NE_BASE = ('https://github.com/nvkelso/natural-earth-vector/raw/master/'
            'geojson/')

print('Loading Natural Earth 50m boundaries...')
_ne_states    = gpd.read_file(_NE_BASE + 'ne_50m_admin_1_states_provinces.geojson')
_ne_countries = gpd.read_file(_NE_BASE + 'ne_50m_admin_0_countries.geojson')

_us_states_gdf = _ne_states[
    (_ne_states['admin'] == 'United States of America') &
    (~_ne_states['name'].isin(['Alaska', 'Hawaii']))
][['name', 'geometry']].copy()

# Country-name column differs between Natural Earth tiers.
_cname_col = next(c for c in ['NAME', 'ADMIN', 'name', 'admin']
                  if c in _ne_countries.columns)
_border_countries = _ne_countries[
    _ne_countries[_cname_col].isin(
        ['United States of America', 'Canada', 'Mexico'])
].copy()

print('  {:d} CONUS state polygons loaded'.format(len(_us_states_gdf)))

# ────────────────────────────────────────────────────────────────────────
# Study domain: WHOLE states, snapped
# ────────────────────────────────────────────────────────────────────────
# The eastern limit of the domain is set by OpenET coverage, which stops near
# 84°W.  Cutting the domain at that meridian draws an artificial straight line
# through the middle of several states.  Instead the domain is defined as a set
# of COMPLETE states, so every edge of the study boundary is a real state or
# national border.
#
# A state joins the domain when it (a) contains at least one valid cropland
# pixel and (b) has at least half its area west of the OpenET cutoff.  Rule (b)
# drops Ohio, Georgia and Florida, which each hold only a thin sliver of pixels
# at the extreme eastern edge; keeping them would push the map ~500 km further
# east for 128 pixels (1.4% of the sample).

EAST_CUTOFF_LON     = float(CONUS_LON[-1]) + 0.0625   # -84.0625°W
MIN_STATE_AREA_WEST = 0.50                            # rule (b)

# Fraction of each state's area west of the OpenET cutoff.
_west_halfplane = _SPoly([(-180, 10), (EAST_CUTOFF_LON, 10),
                          (EAST_CUTOFF_LON, 60), (-180, 60)])
_us_states_gdf['frac_west'] = [
    g.intersection(_west_halfplane).area / g.area
    for g in _us_states_gdf.geometry
]

# Which states hold valid cropland pixels?
_cr, _cc = np.where(crop_mask_static)
_crop_pts = gpd.GeoDataFrame(
    {'row': _cr, 'col': _cc},
    geometry=[Point(float(CONUS_LON[c]), float(CONUS_LAT[r]))
              for r, c in zip(_cr, _cc)],
    crs='EPSG:4326',
)
_pt_state = gpd.sjoin(_crop_pts, _us_states_gdf[['name', 'geometry']],
                      how='left', predicate='within')
_pt_state = _pt_state.drop_duplicates(subset=['row', 'col'])
_state_pixel_counts = _pt_state['name'].value_counts()

_us_states_gdf['npix'] = (_us_states_gdf['name']
                          .map(_state_pixel_counts).fillna(0).astype(int))

_study_states_gdf = _us_states_gdf[
    (_us_states_gdf['npix'] >= 1) &
    (_us_states_gdf['frac_west'] >= MIN_STATE_AREA_WEST)
].copy()
STUDY_STATES = sorted(_study_states_gdf['name'].tolist())

print()
print('Study domain: {:d} whole states'.format(len(STUDY_STATES)))
print('  ' + ', '.join(STUDY_STATES))
_excluded = sorted(set(_us_states_gdf[_us_states_gdf['npix'] >= 1]['name'])
                   - set(STUDY_STATES))
print('  excluded (mostly east of OpenET cutoff): '
      + (', '.join(_excluded) if _excluded else 'none'))

# ── Dissolved study polygon, 4326 and 5070 ───────────────────────────────
_study_union_4326 = _study_states_gdf.geometry.union_all()
_study_states_5070 = _study_states_gdf.to_crs('EPSG:5070')
_states_5070       = _us_states_gdf.to_crs('EPSG:5070')
_countries_5070    = _border_countries.to_crs('EPSG:5070')
_study_area_geom   = gpd.GeoSeries([_study_union_4326],
                                   crs='EPSG:4326').to_crs('EPSG:5070').iloc[0]

# ── Pixel mask: cropland pixels inside the study states ──────────────────
# Buffer by half a grid cell so a coastal cell whose CENTRE falls just offshore
# is still retained.
_HALF_CELL_DEG = 0.0625
_study_buffered = _study_union_4326.buffer(_HALF_CELL_DEG)

_boundary_mask = np.zeros((n_lat, n_lon), dtype=bool)
for r, c in zip(_cr, _cc):
    if _study_buffered.contains(Point(float(CONUS_LON[c]), float(CONUS_LAT[r]))):
        _boundary_mask[r, c] = True

n_inside  = int(_boundary_mask.sum())
n_outside = int(crop_mask_static.sum()) - n_inside
print()
print('Pixel mask: {:,} cropland pixels inside study states, '
      '{:,} clipped out'.format(n_inside, n_outside))

# ────────────────────────────────────────────────────────────────────────
# Apply the same clip to the ANALYSIS SAMPLE
# ────────────────────────────────────────────────────────────────────────
# Previously the boundary clip was applied only when drawing maps, so the
# pooled regression and every pixel-level slope still included cells over the
# ocean, Mexico and Canada that had passed the CDL cropland-fraction threshold.
# Those cells are ~98% empty, but the ~2% carrying values entered the fit.
_keep_px = set(
    (round(float(CONUS_LAT[r]), 4), round(float(CONUS_LON[c]), 4))
    for r, c in zip(*np.where(_boundary_mask))
)
_n_rows_before = len(df)
_px_key = list(zip(df['lat'].round(4), df['lon'].round(4)))
df['in_study'] = [k in _keep_px for k in _px_key]

_cc_before = df.dropna(subset=['sif_z', 'delta_et', 'spei90d']).shape[0]
df = df[df['in_study']].drop(columns='in_study').reset_index(drop=True)
_cc_after = df.dropna(subset=['sif_z', 'delta_et', 'spei90d']).shape[0]

print()
print('Analysis sample clipped to study domain:')
print('  panel rows     : {:,} -> {:,}'.format(_n_rows_before, len(df)))
print('  complete cases : {:,} -> {:,}  ({:,} out-of-domain obs removed)'.format(
    _cc_before, _cc_after, _cc_before - _cc_after))

# ── Map extent — tight to the study domain ───────────────────────────────
_sminx, _sminy, _smaxx, _smaxy = _study_area_geom.bounds
_PAD = 12_000     # 12 km — just enough that the boundary stroke is not clipped
_xmin, _xmax = _sminx - _PAD, _smaxx + _PAD
_ymin, _ymax = _sminy - _PAD, _smaxy + _PAD
print()
print('Map extent (EPSG:5070): x {:,.0f} to {:,.0f} | y {:,.0f} to {:,.0f}'.format(
    _xmin, _xmax, _ymin, _ymax))


# ────────────────────────────────────────────────────────────────────────
# Basemap helpers
# ────────────────────────────────────────────────────────────────────────

def _add_basemap(ax, us_color='white'):
    """White page, white land; state strokes are drawn separately."""
    ax.set_facecolor('white')
    _states_5070.plot(ax=ax, color=us_color, edgecolor='none', zorder=1)
    ax.set_xlim(_xmin, _xmax)
    ax.set_ylim(_ymin, _ymax)


def _draw_states(ax, lw=0.4, color='#333333', zorder=5):
    """Overlay state and country boundaries."""
    _states_5070.boundary.plot(ax=ax, color=color, linewidth=lw, zorder=zorder)
    _countries_5070.boundary.plot(ax=ax, color='#111111', linewidth=lw * 2,
                                  zorder=zorder + 1)


def _draw_study_boundary(ax, lw=2.0, color='#111111', zorder=12):
    """Bold outline of the study domain.

    Every segment is a real state or national border — the domain is a union
    of complete state polygons, so there is no artificial straight edge.
    """
    _bnd = _study_area_geom.boundary
    _geoms = list(_bnd.geoms) if hasattr(_bnd, 'geoms') else [_bnd]
    for _g in _geoms:
        ax.plot(*_g.xy, color=color, linewidth=lw, zorder=zorder,
                solid_capstyle='round', solid_joinstyle='round')


def _albers_axes(ax):
    """Remove ticks, spines, labels; set equal-aspect Albers limits."""
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_xlabel('')
    ax.set_ylabel('')
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_aspect('equal')
    ax.set_xlim(_xmin, _xmax)
    ax.set_ylim(_ymin, _ymax)


def _map_fig(figsize=(9.5, 6.0)):
    """Standard single-map figure."""
    fig, ax = plt.subplots(1, 1, figsize=figsize)
    fig.patch.set_facecolor('white')
    return fig, ax
'''.lstrip()


CELL_FIG01A = r'''
import matplotlib.ticker as mticker

# ── Figure 1a: 10-year mean growing-season Human ET ──────────────────────
# Two colour variants of the same map:
#   default   — ltc 'ploen' reversed (high = blue)
#   heatmap0  — ltc 'heatmap0'

_final_mask = _boundary_mask if _boundary_mask is not None else crop_mask_static
_mean_plot  = np.where(_final_mask, _human_et_mean, np.nan)

plt.rcParams.update({
    'font.family': 'DejaVu Sans', 'font.size': 22,
    'axes.labelsize': 19, 'axes.titlesize': 19,
    'xtick.labelsize': 18, 'ytick.labelsize': 18,
    'legend.fontsize': 12,
})

_FIG01A_VARIANTS = [
    (cmap_ploen_r_map,  'fig01a_human_et_mean'),
    (cmap_heatmap0_map, 'fig01a_human_et_mean_heatmap0'),
]

for _cmap_a, _fname_a in _FIG01A_VARIANTS:
    fig, ax = _map_fig()

    im = ax.pcolormesh(
        _X5070, _Y5070, _mean_plot,
        cmap=_cmap_a, vmin=0, vmax=_mean_vmax,
        shading='nearest', rasterized=True, zorder=2,
    )
    _albers_axes(ax)
    _add_basemap(ax)
    _draw_states(ax, lw=0.4, color='#333333', zorder=5)
    _draw_study_boundary(ax, lw=2.0, zorder=10)

    cb = fig.colorbar(im, ax=ax, orientation='horizontal',
                      fraction=0.04, pad=0.03, shrink=0.72)
    cb.set_label('Mean Human ET [mm month⁻¹]', fontsize=17)
    cb.ax.tick_params(labelsize=15)

    ax.text(0.015, 0.97, '(a)', transform=ax.transAxes,
            fontsize=28, fontweight='bold', va='top', ha='left')

    plt.tight_layout()
    plt.savefig(str(figs / _fname_a) + '.png', dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.savefig(str(figs / _fname_a) + '.pdf', bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.show()
    print('Saved:', _fname_a + '.png / .pdf')
'''.lstrip()


CELL_FIG01B = r'''
# ── Figure 1b: Trend in growing-season Human ET ──────────────────────────
# Two colour variants:
#   default   — RdBu (positive = blue, negative = red)
#   heatmap0  — ltc 'heatmap0'

_finite_slopes = trend_slope[_final_mask & np.isfinite(trend_slope)]
_trend_vmax    = max(float(np.nanpercentile(np.abs(_finite_slopes), 97.5)), 0.10)
_trend_plot    = np.where(_final_mask, trend_slope, np.nan)

plt.rcParams.update({
    'font.family': 'DejaVu Sans', 'font.size': 22,
    'axes.labelsize': 19, 'axes.titlesize': 19,
    'xtick.labelsize': 18, 'ytick.labelsize': 18,
    'legend.fontsize': 12,
})

_FIG01B_VARIANTS = [
    (cmap_rdbu_map,     'fig01b_human_et_trend'),
    (cmap_heatmap0_map, 'fig01b_human_et_trend_heatmap0'),
]

for _cmap_b, _fname_b in _FIG01B_VARIANTS:
    fig, ax = _map_fig()

    im = ax.pcolormesh(
        _X5070, _Y5070, _trend_plot,
        cmap=_cmap_b, vmin=-_trend_vmax, vmax=_trend_vmax,
        shading='nearest', rasterized=True, zorder=2,
    )
    _albers_axes(ax)
    _add_basemap(ax)
    _draw_states(ax, lw=0.4, color='#333333', zorder=5)
    _draw_study_boundary(ax, lw=2.0, zorder=10)

    cb = fig.colorbar(im, ax=ax, orientation='horizontal',
                      fraction=0.04, pad=0.03, shrink=0.72)
    cb.set_label('Trend [mm month⁻¹ yr⁻¹]', fontsize=17)
    cb.ax.tick_params(labelsize=15)
    _tk = [-_trend_vmax, -_trend_vmax / 2, 0, _trend_vmax / 2, _trend_vmax]
    cb.set_ticks(_tk)
    cb.set_ticklabels(['{:.1f}'.format(v) for v in _tk])

    ax.text(0.015, 0.97, '(b)', transform=ax.transAxes,
            fontsize=28, fontweight='bold', va='top', ha='left')

    plt.tight_layout()
    plt.savefig(str(figs / _fname_b) + '.png', dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.savefig(str(figs / _fname_b) + '.pdf', bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.show()
    print('Saved:', _fname_b + '.png / .pdf')
'''.lstrip()


CELL_SCATTER_HELPERS = r'''
from matplotlib.lines import Line2D

# ── Shared helpers for the Figure 1 scatter panels (1c and 1d) ───────────
# Density is conveyed by overplotting: every point is drawn with a low alpha,
# so regions where many pixels coincide accumulate to a darker colour.  This
# replaces the previous solid "blob" of opaque markers.

CA_COLOR = PALETTE_PLOEN[4]   # '#B17776' warm brownish red
IA_COLOR = PALETTE_PLOEN[0]   # '#3F5671' dark blue
OTHER_COLOR = '#9E9E9E'

# alpha, size, z-order per group — tuned so 'other' reads as a density cloud
_SCATTER_STYLE = {
    'other': dict(color=OTHER_COLOR, s=7,  alpha=0.10, zorder=3, label='Other states'),
    'CA':    dict(color=CA_COLOR,    s=13, alpha=0.30, zorder=5, label='California'),
    'IA':    dict(color=IA_COLOR,    s=13, alpha=0.30, zorder=5, label='Iowa'),
}


def _state_label_for_lonlat(lons, lats):
    """Label each (lon, lat) point as 'CA', 'IA' or 'other' via spatial join."""
    lons = np.asarray(lons, dtype=float)
    lats = np.asarray(lats, dtype=float)
    _pts = gpd.GeoDataFrame(
        {'_i': np.arange(len(lons))},
        geometry=[Point(x, y) for x, y in zip(lons, lats)],
        crs='EPSG:4326',
    )
    _j = gpd.sjoin(_pts, _us_states_gdf[['name', 'geometry']],
                   how='left', predicate='within')
    _j = _j.drop_duplicates(subset=['_i']).sort_values('_i')
    _name = _j['name'].values
    return np.where(_name == 'California', 'CA',
                    np.where(_name == 'Iowa', 'IA', 'other'))


def _state_label_for_pixels(rows, cols):
    """Label each (row, col) grid pixel as 'CA', 'IA' or 'other'."""
    return _state_label_for_lonlat(
        [float(CONUS_LON[c]) for c in cols],
        [float(CONUS_LAT[r]) for r in rows],
    )


def _draw_fit(ax, x, y, color, min_n=25):
    """Least-squares fit line drawn across the observed x-range."""
    ok = np.isfinite(x) & np.isfinite(y)
    if ok.sum() < min_n:
        return None
    slope, intercept, r, p, se = stats.linregress(x[ok], y[ok])
    xs = np.linspace(np.nanmin(x[ok]), np.nanmax(x[ok]), 100)
    ax.plot(xs, intercept + slope * xs, color=color, linewidth=3.0,
            zorder=8, solid_capstyle='round')
    # White casing underneath so the line stays legible over dense points
    ax.plot(xs, intercept + slope * xs, color='white', linewidth=5.0,
            zorder=7, solid_capstyle='round')
    return slope, intercept, r, p, se


def _scatter_panel(ax, df_pts, xcol, ycol, xlabel, ylabel, panel_letter):
    """Shared scatter rendering for fig01c / fig01d."""
    for key in ['other', 'CA', 'IA']:
        sub = df_pts[df_pts['state'] == key]
        st  = _SCATTER_STYLE[key]
        ax.scatter(sub[xcol], sub[ycol], c=st['color'], s=st['s'],
                   alpha=st['alpha'], linewidths=0, zorder=st['zorder'],
                   rasterized=True)

    ax.axhline(0, color='#444444', lw=1.2, linestyle='--', zorder=2)

    fits = {}
    for key, color in [('CA', CA_COLOR), ('IA', IA_COLOR)]:
        sub = df_pts[df_pts['state'] == key]
        fit = _draw_fit(ax, sub[xcol].values, sub[ycol].values, color)
        if fit is not None:
            fits[key] = fit

    ax.set_xlabel(xlabel, fontsize=19)
    ax.set_ylabel(ylabel, fontsize=19)
    ax.tick_params(labelsize=16)
    ax.grid(alpha=0.2)

    # Legend proxies at full opacity (the plotted points are deliberately faint)
    _handles = [
        Line2D([], [], marker='o', linestyle='none', markersize=9,
               markerfacecolor=_SCATTER_STYLE[k]['color'],
               markeredgecolor='none', label=_SCATTER_STYLE[k]['label'])
        for k in ['other', 'CA', 'IA']
    ]
    ax.legend(handles=_handles, fontsize=15, framealpha=0.92,
              loc='upper right', borderpad=0.5, handletextpad=0.4)

    ax.text(0.015, 0.97, panel_letter, transform=ax.transAxes,
            fontsize=28, fontweight='bold', va='top', ha='left')
    return fits


def _report_fits(fits, label):
    for key, (slope, intercept, r, p, se) in fits.items():
        print('  {:12s} {:3s}: slope = {:+.4f}  r = {:+.3f}  p = {:.3g}'.format(
            label, key, slope, r, p))
'''.lstrip()


CELL_FIG01C = r'''
# ── Figure 1c: Scatter — 10-yr mean Human ET vs trend ────────────────────
_rows_c, _cols_c = np.where(_final_mask & np.isfinite(_human_et_mean)
                            & np.isfinite(trend_slope))

df_sc = pd.DataFrame({
    'mean_et': _human_et_mean[_rows_c, _cols_c],
    'trend':   trend_slope[_rows_c, _cols_c],
    'state':   _state_label_for_pixels(_rows_c, _cols_c),
})

print('Figure 1c pixels: {:,} total | CA {:,} | Iowa {:,} | other {:,}'.format(
    len(df_sc),
    int((df_sc['state'] == 'CA').sum()),
    int((df_sc['state'] == 'IA').sum()),
    int((df_sc['state'] == 'other').sum()),
))

plt.rcParams.update({
    'font.family': 'DejaVu Sans', 'font.size': 22,
    'axes.labelsize': 19, 'axes.titlesize': 19,
    'xtick.labelsize': 16, 'ytick.labelsize': 16,
    'legend.fontsize': 15,
})

fig, ax = plt.subplots(figsize=(8.5, 6.5))
fig.patch.set_facecolor('white')

_fits_c = _scatter_panel(
    ax, df_sc, 'mean_et', 'trend',
    '10-yr mean growing-season Human ET [mm month⁻¹]',
    'Trend in Human ET [mm month⁻¹ yr⁻¹]',
    '(c)',
)
_report_fits(_fits_c, 'fig01c')

plt.tight_layout()
_fig01c = figs / 'fig01c_mean_vs_trend_scatter'
plt.savefig(str(_fig01c) + '.png', dpi=300, bbox_inches='tight',
            facecolor='white', edgecolor='none')
plt.savefig(str(_fig01c) + '.pdf', bbox_inches='tight',
            facecolor='white', edgecolor='none')
plt.show()
print('Saved: fig01c_mean_vs_trend_scatter.png / .pdf')
'''.lstrip()


CELL_FIG01D = r'''
# ── Figure 1d: First-year Human ET vs trend ──────────────────────────────
# Panel (d) of Figure 1.  x-axis is the FIRST year of record (2015) growing-
# season mean Human ET, so the panel reads as "where irrigation started high,
# which way has it moved since?"  Same CA / Iowa case-study colouring and
# density treatment as panel (c).

_first_year   = YEARS[0]
_first_year_et = gs_stack[0]          # (n_lat, n_lon) — YEARS[0] GS mean

_rows_d, _cols_d = np.where(_final_mask & np.isfinite(_first_year_et)
                            & np.isfinite(trend_slope))

df_sd = pd.DataFrame({
    'first_et': _first_year_et[_rows_d, _cols_d],
    'trend':    trend_slope[_rows_d, _cols_d],
    'state':    _state_label_for_pixels(_rows_d, _cols_d),
})

print('Figure 1d pixels ({} baseline): {:,} total | CA {:,} | Iowa {:,}'.format(
    _first_year, len(df_sd),
    int((df_sd['state'] == 'CA').sum()),
    int((df_sd['state'] == 'IA').sum()),
))

plt.rcParams.update({
    'font.family': 'DejaVu Sans', 'font.size': 22,
    'axes.labelsize': 19, 'axes.titlesize': 19,
    'xtick.labelsize': 16, 'ytick.labelsize': 16,
    'legend.fontsize': 15,
})

fig, ax = plt.subplots(figsize=(8.5, 6.5))
fig.patch.set_facecolor('white')

_fits_d = _scatter_panel(
    ax, df_sd, 'first_et', 'trend',
    '{} mean growing-season Human ET [mm month⁻¹]'.format(_first_year),
    'Trend in Human ET [mm month⁻¹ yr⁻¹]',
    '(d)',
)
_report_fits(_fits_d, 'fig01d')

plt.tight_layout()
_fig01d = figs / 'fig01d_firstyear_vs_trend_scatter'
plt.savefig(str(_fig01d) + '.png', dpi=300, bbox_inches='tight',
            facecolor='white', edgecolor='none')
plt.savefig(str(_fig01d) + '.pdf', bbox_inches='tight',
            facecolor='white', edgecolor='none')
plt.show()
print('Saved: fig01d_firstyear_vs_trend_scatter.png / .pdf')
'''.lstrip()


CELL_FIG02 = r'''
# ── Figure 2: Four separate slope maps — one per drought category ──────────
# ltc 'ploen' reversed (positive slope = blue). Significant pixels only.
# Files: fig02_slope_{no_drought,mild,moderate,severe}

from matplotlib.patches import Patch

plt.rcParams.update({
    'font.family': 'DejaVu Sans', 'font.size': 22,
    'axes.labelsize': 19, 'axes.titlesize': 19,
    'xtick.labelsize': 18, 'ytick.labelsize': 18,
    'legend.fontsize': 12,
})

_use_final_mask = _boundary_mask if _boundary_mask is not None else crop_mask_static
_lat_to_row = {round(float(v), 4): i for i, v in enumerate(CONUS_LAT)}
_lon_to_col = {round(float(v), 4): i for i, v in enumerate(CONUS_LON)}

# Shared symmetric color range across all 4 maps (±97.5th pct of all sig slopes)
_all_slopes = np.concatenate([
    pix_slopes_by_cat[cn]['slope'].dropna().values
    for cn in pix_slopes_by_cat
])
_sv_max = max(float(np.percentile(np.abs(_all_slopes), 97.5)), 0.005)

_CAT_LABELS = [
    ('No Drought\n(SPEI>-0.5)',         'SPEI > −0.5',       'fig02_slope_no_drought'),
    ('Mild\n(-1.0<SPEI≤0.5)',   '−1.0 < SPEI ≤ −0.5', 'fig02_slope_mild'),
    ('Moderate\n(-1.5<SPEI≤1.0)', '−1.5 < SPEI ≤ −1.0', 'fig02_slope_moderate'),
    ('Severe\n(SPEI≤-1.5)',         'SPEI ≤ −1.5',   'fig02_slope_severe'),
]

for (cat_key, cat_title, fname) in _CAT_LABELS:
    pix_cat = pix_slopes_by_cat.get(cat_key, pd.DataFrame())

    _slope_grid = np.full((n_lat, n_lon), np.nan)
    for _, row in pix_cat.iterrows():
        ri = _lat_to_row.get(round(float(row['lat']), 4))
        ci = _lon_to_col.get(round(float(row['lon']), 4))
        if ri is not None and ci is not None:
            _slope_grid[ri, ci] = row['slope']

    _slope_plot = np.where(_use_final_mask, _slope_grid, np.nan)

    _cmap_s = cmap_ploen_r_map.copy()
    _cmap_s.set_bad('#888888', alpha=1.0)  # non-sig = medium grey

    fig, ax = _map_fig()

    im = ax.pcolormesh(
        _X5070, _Y5070, _slope_plot,
        cmap=_cmap_s, vmin=-_sv_max, vmax=_sv_max,
        shading='nearest', rasterized=True, zorder=2,
    )
    _albers_axes(ax)
    _add_basemap(ax)
    _draw_states(ax, lw=0.4, color='#333333', zorder=5)
    _draw_study_boundary(ax, lw=2.0, zorder=10)

    cb = fig.colorbar(im, ax=ax, orientation='horizontal',
                      fraction=0.04, pad=0.03, shrink=0.72)
    cb.set_label('Slope: SIF z-score per mm month⁻¹ Human ET',
                 fontsize=17)
    cb.ax.tick_params(labelsize=15)
    cb.set_ticks([-_sv_max, 0, _sv_max])
    cb.set_ticklabels(['{:.3f}'.format(-_sv_max), '0', '{:.3f}'.format(_sv_max)])

    _leg = [Patch(facecolor='#888888', edgecolor='none',
                  label='Not significant / < {:d} obs'.format(MIN_OBS_PIXEL))]
    ax.legend(handles=_leg, loc='lower left', fontsize=14,
              framealpha=0.9, handlelength=1.2, borderpad=0.5)

    ax.text(0.015, 0.97, cat_title, transform=ax.transAxes,
            fontsize=20, fontweight='bold', va='top', ha='left',
            bbox=dict(boxstyle='round,pad=0.25', facecolor='white', alpha=0.8,
                      edgecolor='none'))

    ax.text(0.99, 0.03, 'n={:,} sig. pixels'.format(len(pix_cat)),
            transform=ax.transAxes, fontsize=13, ha='right', va='bottom',
            color='#555555')

    plt.tight_layout()
    plt.savefig(str(figs / fname) + '.png', dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.savefig(str(figs / fname) + '.pdf', bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.show()
    print('Saved:', fname + '.png / .pdf')

print()
print('Figure 2 complete: 4 maps exported (SPEI-90d).')
'''.lstrip()


CELL_FIG02B = r'''
# ── Figure 2b: Bivariate maps — buffering slope x drought severity ────────
# Each pixel is classified into terciles on two independent axes and given one
# of 9 colours from a 2-D grid:
#
#   x-axis : per-pixel OLS slope of SIF z-score on Human ET  (buffering)
#   y-axis : per-pixel mean drought index, inverted so higher = drier
#
# A 1-D ramp such as heatmap0 cannot encode two axes, so the grid is built by
# bilinear interpolation in RGB between four corner colours drawn from
# heatmap0.  Each axis then varies independently and the legend reads as a
# proper 3x3 square.

from matplotlib.colors import to_rgb, to_hex, ListedColormap, BoundaryNorm

# Corner colours (all from PALETTE_HEATMAP0, plus a light neutral tint)
_BIV_C00 = '#EDE7D9'   # low slope,  low drought  — light neutral (E9D8A6 tint)
_BIV_C10 = '#0A9396'   # high slope, low drought  — teal
_BIV_C01 = '#AE2012'   # low slope,  high drought — red
_BIV_C11 = '#001219'   # high slope, high drought — near-black


def _bivariate_grid(c00, c10, c01, c11, n=3):
    """Bilinear RGB blend of four corners into an n x n colour grid."""
    a, b, c, d = (np.array(to_rgb(x)) for x in (c00, c10, c01, c11))
    grid = np.empty((n, n), dtype=object)
    for j in range(n):                       # j = drought tercile (y)
        fy = j / (n - 1)
        for i in range(n):                   # i = slope tercile (x)
            fx = i / (n - 1)
            rgb = (a * (1 - fx) * (1 - fy) + b * fx * (1 - fy)
                   + c * (1 - fx) * fy + d * fx * fy)
            grid[j, i] = to_hex(np.clip(rgb, 0, 1))
    return grid


BIV_GRID = _bivariate_grid(_BIV_C00, _BIV_C10, _BIV_C01, _BIV_C11, n=3)
print('Bivariate 3x3 colour grid (rows = drought tercile, cols = slope tercile):')
for j in range(3):
    print('  ' + '  '.join(BIV_GRID[j, i] for i in range(3)))


def _add_bivariate_legend(fig, grid, xlabel, ylabel,
                          rect=(0.055, 0.13, 0.15, 0.15)):
    """Draw the 3x3 key as an inset axes with arrow labels on both axes."""
    lax = fig.add_axes(rect)
    n = grid.shape[0]
    for j in range(n):
        for i in range(n):
            lax.add_patch(plt.Rectangle((i, j), 1, 1, facecolor=grid[j, i],
                                        edgecolor='white', linewidth=1.2))
    lax.set_xlim(0, n)
    lax.set_ylim(0, n)
    lax.set_xticks([])
    lax.set_yticks([])
    for s in lax.spines.values():
        s.set_visible(False)
    lax.set_aspect('equal')
    lax.set_xlabel(xlabel, fontsize=12, labelpad=4)
    lax.set_ylabel(ylabel, fontsize=12, labelpad=4)
    lax.annotate('', xy=(n, -0.25), xytext=(0, -0.25),
                 annotation_clip=False,
                 arrowprops=dict(arrowstyle='->', lw=1.2, color='#333333'))
    lax.annotate('', xy=(-0.25, n), xytext=(-0.25, 0),
                 annotation_clip=False,
                 arrowprops=dict(arrowstyle='->', lw=1.2, color='#333333'))
    return lax


def _pixel_slope_table(df_in, index_col):
    """Per-pixel slope of sif_z on delta_et, plus mean drought index."""
    sub = df_in.dropna(subset=['sif_z', 'delta_et', index_col]).copy()
    sub = sub[np.isfinite(sub['sif_z']) & np.isfinite(sub['delta_et'])
              & np.isfinite(sub[index_col])]

    out = []
    for (plat, plon), grp in sub.groupby(['lat', 'lon']):
        if len(grp) < MIN_OBS_PIXEL:
            continue
        x = grp['delta_et'].values
        y = grp['sif_z'].values
        if np.nanstd(x) == 0:
            continue
        slope, _, _, pval, _ = stats.linregress(x, y)
        out.append({'lat': plat, 'lon': plon, 'slope': slope, 'pval': pval,
                    'drought_mean': float(grp[index_col].mean()),
                    'n': len(grp)})
    return pd.DataFrame(out)


def _make_bivariate_map(df_in, index_col, index_label, fsuffix):
    tbl = _pixel_slope_table(df_in, index_col)
    if len(tbl) < 100:
        print('  {}: only {} pixels — skipped'.format(index_label, len(tbl)))
        return

    # Terciles. Drought axis is INVERTED (more negative index = drier = higher
    # tercile) so that "up" on the legend always means "more drought".
    tbl['x_ter'] = pd.qcut(tbl['slope'], 3, labels=False)
    tbl['y_ter'] = pd.qcut(-tbl['drought_mean'], 3, labels=False)

    _codes = np.full((n_lat, n_lon), -1, dtype=int)
    _lat_to_row_b = {round(float(v), 4): i for i, v in enumerate(CONUS_LAT)}
    _lon_to_col_b = {round(float(v), 4): i for i, v in enumerate(CONUS_LON)}
    for _, r in tbl.iterrows():
        ri = _lat_to_row_b.get(round(float(r['lat']), 4))
        ci = _lon_to_col_b.get(round(float(r['lon']), 4))
        if ri is not None and ci is not None:
            _codes[ri, ci] = int(r['y_ter']) * 3 + int(r['x_ter'])

    _codes = np.where(_use_final_mask, _codes, -1)

    # 9 discrete classes -> ListedColormap indexed by the class code.
    _flat = [BIV_GRID[j, i] for j in range(3) for i in range(3)]
    _cmap_biv = ListedColormap(_flat)
    _cmap_biv.set_bad('white', alpha=0)
    _norm_biv = BoundaryNorm(np.arange(-0.5, 9.5, 1.0), _cmap_biv.N)

    fig, ax = _map_fig(figsize=(9.5, 6.4))
    _albers_axes(ax)
    _add_basemap(ax)
    ax.pcolormesh(_X5070, _Y5070, np.ma.masked_where(_codes < 0, _codes),
                  cmap=_cmap_biv, norm=_norm_biv, shading='nearest',
                  rasterized=True, zorder=2)
    _draw_states(ax, lw=0.4, color='#333333', zorder=5)
    _draw_study_boundary(ax, lw=2.0, zorder=10)

    ax.text(0.015, 0.97, index_label, transform=ax.transAxes,
            fontsize=20, fontweight='bold', va='top', ha='left',
            bbox=dict(boxstyle='round,pad=0.25', facecolor='white', alpha=0.8,
                      edgecolor='none'))
    ax.text(0.99, 0.03, 'n={:,} pixels'.format(len(tbl)),
            transform=ax.transAxes, fontsize=13, ha='right', va='bottom',
            color='#555555')

    _add_bivariate_legend(
        fig, BIV_GRID,
        'Buffering slope →',
        'Drought severity →',
    )

    fname = 'fig02b_bivariate_{}'.format(fsuffix)
    plt.savefig(str(figs / fname) + '.png', dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.savefig(str(figs / fname) + '.pdf', bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.show()
    print('  Saved:', fname + '.png / .pdf')


plt.rcParams.update({
    'font.family': 'DejaVu Sans', 'font.size': 22,
    'axes.labelsize': 19, 'axes.titlesize': 19,
    'xtick.labelsize': 16, 'ytick.labelsize': 16,
    'legend.fontsize': 14,
})

_BIV_INDICES = [
    ('spei30d',  'SPEI-30d',  'spei30d'),
    ('spei90d',  'SPEI-90d',  'spei90d'),
    ('spei180d', 'SPEI-180d', 'spei180d'),
    ('rzsm_z',   'RZSM z',    'rzsm_z'),
]

print()
for _col, _lbl, _suf in _BIV_INDICES:
    if _col not in df.columns or df[_col].notna().sum() == 0:
        print('  {}: column not available — skipped'.format(_lbl))
        continue
    _make_bivariate_map(df, _col, _lbl, _suf)

print()
print('Figure 2b complete.')
'''.lstrip()


CELL_FIG03 = r'''
plt.rcParams.update({
    'font.family': 'DejaVu Sans', 'font.size': 22,
    'axes.labelsize': 19, 'axes.titlesize': 19,
    'xtick.labelsize': 18, 'ytick.labelsize': 18,
    'legend.fontsize': 12,
})

# ── Attach state labels so CA / Iowa case-study panels can be split out ──
if 'state' not in _df_fig.columns:
    _fig_px = _df_fig[['lat', 'lon']].drop_duplicates().reset_index(drop=True)
    _fig_px['state'] = _state_label_for_lonlat(_fig_px['lon'].values,
                                               _fig_px['lat'].values)
    _df_fig = _df_fig.merge(_fig_px, on=['lat', 'lon'], how='left')

print('Figure 3 subsets: CA {:,} obs | Iowa {:,} obs | all {:,} obs'.format(
    int((_df_fig['state'] == 'CA').sum()),
    int((_df_fig['state'] == 'IA').sum()),
    len(_df_fig),
))


def _fig03_lines(df_src, fname, title=None, n_bins=20):
    """Line plot of mean SIF z-score vs Human ET, one line per drought category.

    Legend sits inside the axes at lower right, sized small enough to clear
    the lines (which rise to the upper right).
    """
    _df_pos = df_src[df_src['delta_et'] >= 0].copy()
    if len(_df_pos) < 500:
        print('  {}: only {:,} obs — skipped'.format(fname, len(_df_pos)))
        return

    _nb = min(n_bins, max(4, len(_df_pos) // 200))
    _df_pos['et_bin'] = pd.qcut(_df_pos['delta_et'], q=_nb,
                                labels=False, duplicates='drop') + 1
    _bin_median_et = _df_pos.groupby('et_bin', observed=True)['delta_et'].median()

    _grp3 = _df_pos.groupby(['et_bin', 'drought_cat'], observed=True)
    _summary3 = _grp3['sif_z'].agg(['mean', 'sem', 'count']).reset_index()
    _summary3.columns = ['et_bin', 'drought_cat', 'mean_sifz', 'se_sifz', 'n']
    _summary3['et_mm'] = _summary3['et_bin'].map(_bin_median_et)

    fig, ax = plt.subplots(figsize=(9, 5.5))
    fig.patch.set_facecolor('white')

    _drought_order3 = list(reversed(_drought_labels))
    for cat in _drought_order3:
        _sub = _summary3[_summary3['drought_cat'] == cat].sort_values('et_mm')
        if len(_sub) == 0:
            continue
        col = DROUGHT_COLORS_LTC.get(cat, 'gray')
        ax.plot(_sub['et_mm'], _sub['mean_sifz'], color=col, linewidth=2.4,
                label=cat.replace('\n', ' '), zorder=5)
        ax.fill_between(_sub['et_mm'],
                        _sub['mean_sifz'] - _sub['se_sifz'],
                        _sub['mean_sifz'] + _sub['se_sifz'],
                        color=col, alpha=0.12, zorder=2)

    ax.axhline(0, color='#333333', linewidth=2.0, linestyle='-', zorder=3)
    ax.set_xlabel('Human ET [mm month⁻¹]', fontsize=19)
    ax.set_ylabel('Mean SIF z-score', fontsize=19)
    ax.set_xlim(left=0)
    ax.grid(alpha=0.25)

    if title:
        ax.set_title(title, fontsize=19, pad=8)

    # Rug marks: data density along the x-axis
    _rug_y = ax.get_ylim()[0]
    _rug = _df_pos['delta_et'].sample(n=min(3000, len(_df_pos)),
                                      random_state=42).values
    ax.plot(_rug, np.full_like(_rug, _rug_y), '|', color='#666666',
            markersize=4, markeredgewidth=0.5, alpha=0.18, zorder=1)
    _y0, _y1 = ax.get_ylim()
    ax.set_ylim(_y0 - 0.03 * (_y1 - _y0), _y1)

    # Legend inside the frame, lower right — compact so it clears the lines
    ax.legend(title='Drought severity', fontsize=11, title_fontsize=11,
              loc='lower right', framealpha=0.92, borderpad=0.5,
              labelspacing=0.35, handlelength=1.4, handletextpad=0.5)

    plt.tight_layout()
    plt.savefig(str(figs / fname) + '.png', dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.savefig(str(figs / fname) + '.pdf', bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.show()
    print('  Saved:', fname + '.png / .pdf')


# Full domain + the two case-study states
_fig03_lines(_df_fig, 'fig03_percentile_lines')
_fig03_lines(_df_fig[_df_fig['state'] == 'CA'],
             'fig03_percentile_lines_california', title='California')
_fig03_lines(_df_fig[_df_fig['state'] == 'IA'],
             'fig03_percentile_lines_iowa', title='Iowa')

print()
print('Figure 3 complete: full domain + California + Iowa.')
'''.lstrip()


CELL_FE_MD = r'''
---

## 8. Controlling for Location: Fixed-Effects Regressions

The pooled OLS behind Figure 3 treats every pixel-month as an independent
observation. That leaves two confounds in place:

1. **Geographic bias.** Irrigated pixels differ from rainfed pixels in soil,
   crop mix, elevation and climate normals. A positive Human ET coefficient
   could reflect *where* irrigation happens rather than what irrigation *does*.
2. **Year shocks.** A single unusually wet or dry year shifts SPEI and SIF
   together across the whole domain.

A two-way fixed-effects panel addresses both. Pixel fixed effects absorb every
time-invariant property of a location, so identification comes only from
variation *within* a pixel over time — each pixel is compared against itself.
Year fixed effects absorb domain-wide annual shocks; month effects retain the
seasonal control from the original model. Standard errors are clustered by
pixel, which is also a partial answer to spatial autocorrelation: repeat
observations of the same pixel are no longer treated as independent.

Pixel effects are absorbed by within-transformation (demeaning each variable on
its pixel) rather than by ~7,000 dummy columns. The reported R-squared for the
FE model is therefore a **within** R-squared and is not directly comparable to
the pooled R-squared.
'''.lstrip()


CELL_FE = r'''
# ── Fixed-effects regressions controlling for location ───────────────────
import statsmodels.api as sm

_fe_df = df.dropna(subset=['sif_z', 'delta_et', 'spei90d']).copy()
_fe_df = _fe_df[np.isfinite(_fe_df['sif_z']) & np.isfinite(_fe_df['delta_et'])
                & np.isfinite(_fe_df['spei90d'])].copy()
_fe_df['spei_x_det'] = _fe_df['spei90d'] * _fe_df['delta_et']
_fe_df['pix'] = (_fe_df['lat'].round(4).astype(str) + '_'
                 + _fe_df['lon'].round(4).astype(str))

print('FE sample: {:,} obs across {:,} pixels'.format(
    len(_fe_df), _fe_df['pix'].nunique()))

_TERMS = ['spei90d', 'delta_et', 'spei_x_det']
_PRETTY = {'spei90d': 'SPEI-90d', 'delta_et': 'Human ET (dET)',
           'spei_x_det': 'SPEI x Human ET'}


def _run(y, X, label, cluster=None, note=''):
    res = (sm.OLS(y, X).fit(cov_type='cluster', cov_kwds={'groups': cluster})
           if cluster is not None else sm.OLS(y, X).fit(cov_type='HC3'))
    print()
    print('=' * 68)
    print(label)
    print('  N = {:,}   R2 = {:.4f}   {}'.format(int(res.nobs), res.rsquared, note))
    print('-' * 68)
    for t in _TERMS:
        if t in res.params.index:
            b, se, p = res.params[t], res.bse[t], res.pvalues[t]
            stars = ('***' if p < .001 else '**' if p < .01
                     else '*' if p < .05 else '')
            print('  {:18s} b = {: .6f}   SE = {:.6f}   p = {:.3g} {}'.format(
                _PRETTY[t], b, se, p, stars))
    return res


# ── Model 1: pooled OLS (the Figure 3 specification) ─────────────────────
_m_d = pd.get_dummies(_fe_df['month'], prefix='m', drop_first=True).astype(float)
_X1 = sm.add_constant(pd.concat([_fe_df[_TERMS].astype(float), _m_d], axis=1))
_res_pooled = _run(_fe_df['sif_z'].astype(float), _X1,
                   'Model 1 — Pooled OLS, month FE, HC3 robust SE')

# ── Model 2: + year FE ───────────────────────────────────────────────────
_y_d = pd.get_dummies(_fe_df['year'], prefix='y', drop_first=True).astype(float)
_X2 = sm.add_constant(pd.concat([_fe_df[_TERMS].astype(float), _m_d, _y_d], axis=1))
_res_year = _run(_fe_df['sif_z'].astype(float), _X2,
                 'Model 2 — + year FE, clustered by pixel',
                 cluster=_fe_df['pix'].values)

# ── Model 3: pixel + year + month FE (within-transformed) ────────────────
_work = pd.concat([_fe_df[['sif_z'] + _TERMS].astype(float), _m_d, _y_d], axis=1)
_cols = list(_work.columns)
_work['pix'] = _fe_df['pix'].values
_demeaned = _work[_cols] - _work.groupby('pix', sort=False)[_cols].transform('mean')

_res_fe = _run(_demeaned['sif_z'],
               _demeaned[[c for c in _cols if c != 'sif_z']],
               'Model 3 — Pixel + year + month FE, clustered by pixel',
               cluster=_fe_df['pix'].values,
               note='(within R2; {:,} pixel effects absorbed)'.format(
                   _fe_df['pix'].nunique()))

# ── Marginal effect of irrigation across drought severity ────────────────
print()
print('=' * 68)
print('Marginal effect of +1 mm month-1 Human ET on SIF z-score')
print('  dSIF/dET = b_ET + b_interaction * SPEI')
print('-' * 68)
print('  {:<22s} {:>12s} {:>12s} {:>12s}'.format(
    'SPEI', 'Pooled', '+ year FE', 'Pixel+year FE'))
for _spei, _lbl in [(0.0, 'No drought (0)'), (-0.5, 'Mild (-0.5)'),
                    (-1.0, 'Moderate (-1.0)'), (-1.5, 'Severe (-1.5)'),
                    (-2.0, 'Extreme (-2.0)')]:
    _vals = [r.params['delta_et'] + r.params['spei_x_det'] * _spei
             for r in (_res_pooled, _res_year, _res_fe)]
    print('  {:<22s} {:>12.5f} {:>12.5f} {:>12.5f}'.format(_lbl, *_vals))

_b_et = _res_fe.params['delta_et']
_b_ix = _res_fe.params['spei_x_det']
_drop = 1 - (_b_et + _b_ix * -2.0) / _b_et
print()
print('Pixel+year FE: irrigation benefit at SPEI = -2.0 is {:.0f}% smaller'.format(
    100 * _drop))
print('than under no drought — the buffering-breakdown result, now net of')
print('every time-invariant difference between locations.')

# ── Export comparison table ──────────────────────────────────────────────
_rows_tab = []
for _lbl, _r in [('Pooled OLS', _res_pooled), ('+ Year FE', _res_year),
                 ('Pixel + Year FE', _res_fe)]:
    _row = {'Model': _lbl, 'N': int(_r.nobs), 'R2': round(_r.rsquared, 4)}
    for _t in _TERMS:
        _row[_PRETTY[_t]] = '{:.5f}'.format(_r.params[_t])
        _row[_PRETTY[_t] + ' SE'] = '{:.5f}'.format(_r.bse[_t])
    _rows_tab.append(_row)

_tab_fe = pd.DataFrame(_rows_tab)
_tab_fe.to_csv(figs / 'table_fixed_effects_comparison.csv', index=False)
print()
print('Saved: table_fixed_effects_comparison.csv')
_tab_fe
'''.lstrip()


# =============================================================================
# Apply
# =============================================================================

def main():
    nb = json.loads(NB.read_text())
    cells = nb['cells']
    by_id = {c.get('id'): i for i, c in enumerate(cells)}

    def replace(cell_id, source, what):
        i = by_id[cell_id]
        cells[i]['source'] = source.splitlines(keepends=True)
        cells[i]['outputs'] = []
        cells[i]['execution_count'] = None
        print('  replaced {}  ({})'.format(cell_id, what))

    def insert_after(cell_id, source, cell_type, new_id, what):
        i = by_id[cell_id]
        cell = {
            'id': new_id,
            'cell_type': cell_type,
            'metadata': {},
            'source': source.splitlines(keepends=True),
        }
        if cell_type == 'code':
            cell['outputs'] = []
            cell['execution_count'] = None
        cells.insert(i + 1, cell)
        by_id.clear()
        by_id.update({c.get('id'): k for k, c in enumerate(cells)})
        print('  inserted {} after {}  ({})'.format(new_id, cell_id, what))

    print('Patching', NB.name)

    replace('63e90327', CELL_BASEMAP, 'basemap + whole-state study boundary + sample clip')
    replace('6ef66d19', CELL_FIG01A, 'fig01a + heatmap0 variant')
    replace('7ed929df', CELL_FIG01B, 'fig01b + heatmap0 variant')

    # Scatter helpers must exist before fig01c uses them.
    if 'sc_helpers' not in by_id:
        insert_after('7ed929df', CELL_SCATTER_HELPERS, 'code', 'sc_helpers',
                     'shared scatter helpers')
    else:
        replace('sc_helpers', CELL_SCATTER_HELPERS, 'shared scatter helpers')

    replace('e6d62398', CELL_FIG01C, 'fig01c density + CA/IA fit lines')

    if 'fig01d_cell' not in by_id:
        insert_after('e6d62398', CELL_FIG01D, 'code', 'fig01d_cell',
                     'fig01d first-year vs trend')
    else:
        replace('fig01d_cell', CELL_FIG01D, 'fig01d first-year vs trend')

    replace('51e4eb91', CELL_FIG02, 'fig02 formatting')

    if 'fig02b_cell' not in by_id:
        insert_after('51e4eb91', CELL_FIG02B, 'code', 'fig02b_cell',
                     'fig02b bivariate maps')
    else:
        replace('fig02b_cell', CELL_FIG02B, 'fig02b bivariate maps')

    replace('4d40b549', CELL_FIG03, 'fig03 legend + CA/IA case studies')

    # Fixed-effects section at the end of the notebook.
    _last = cells[-1].get('id')
    if 'fe_md' not in by_id:
        insert_after(_last, CELL_FE_MD, 'markdown', 'fe_md', 'FE section intro')
        insert_after('fe_md', CELL_FE, 'code', 'fe_cell', 'FE regressions')
    else:
        replace('fe_md', CELL_FE_MD, 'FE section intro')
        replace('fe_cell', CELL_FE, 'FE regressions')

    NB.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + '\n')
    print()
    print('Wrote', NB)
    print('Total cells:', len(cells))


if __name__ == '__main__':
    main()
