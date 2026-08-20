#!/usr/bin/env python3
"""
Patch 03_visualizations_conus.ipynb with aesthetic updates:
  1. Double all font sizes (rcParams + explicit fontsize= kwargs)
  2. White map background (remove #DCDCDC state fill)
  3. Fix eastern cutoff line (clip to US boundary instead of floating)
  4. Reverse Human ET colormap (high = blue, low = red)
  5. Switch slope/trend maps to RdBu (positive = blue, negative = red)
  6. Non-significant pixels -> darker grey (#888888)
  7. Fig 3 legend: move up from 'lower right' to 'upper left'

Run from the project root:
  /home/pielab-sandbox-jcoldiron/.venv/bin/python3 src/scripts/patch_viz_notebook.py

After running, re-execute these cells in the notebook (by ID or section):
  - ce29a1bd  (palette cell)
  - 63e90327  (basemap helpers)
  - e14564eb  (Fig 1a — mean ET map)
  - 2dfe2523  (Fig 1b — trend map)
  - aab2d65c  (Fig 1c — scatter)
  - 51e4eb91  (Fig 2 — 4 slope maps)
  - 4d40b549  (Fig 3 — line plot)
  - 5a8ac2b0+ (Section 7 multi-index variants)
"""

import json
import re
from pathlib import Path

NB_PATH = Path(__file__).resolve().parent.parent.parent / \
    'src' / 'notebooks' / 'conus' / '03_visualizations_conus.ipynb'

print(f'Patching: {NB_PATH}')
nb = json.loads(NB_PATH.read_text())

# ── Helper: apply a list of (old, new) substitutions to a cell's source ───
def patch_cell(cell, subs):
    src = ''.join(cell['source'])
    for old, new in subs:
        if old in src:
            src = src.replace(old, new)
        else:
            print(f'  WARNING: pattern not found:\n    {old!r}')
    cell['source'] = [line + '\n' for line in src.split('\n')]
    # Fix the last element — split adds an empty string after trailing \n
    if cell['source'] and cell['source'][-1] == '\n':
        cell['source'][-1] = ''
    return cell

# ── Build a lookup: cell_id -> cell object ─────────────────────────────────
cells_by_id = {c.get('id', ''): c for c in nb['cells']}

def get_cell(cid):
    c = cells_by_id.get(cid)
    if c is None:
        print(f'WARNING: cell {cid} not found')
    return c


# ══════════════════════════════════════════════════════════════════════════
# 1. PALETTE CELL (ce29a1bd) — add cmap_rdbu_map + reversed ploen
# ══════════════════════════════════════════════════════════════════════════
c = get_cell('ce29a1bd')
if c:
    patch_cell(c, [
        # After setting up the NaN-transparent copies, add RdBu diverging map
        (
            "cmap_heatmap0_map = cmap_heatmap0.copy(); cmap_heatmap0_map.set_bad('white', alpha=0)",
            "cmap_heatmap0_map = cmap_heatmap0.copy(); cmap_heatmap0_map.set_bad('white', alpha=0)\n"
            "# Red→White→Blue diverging map (negative=red, positive=blue)\n"
            "# Used for trend maps and slope maps\n"
            "cmap_rdbu_map = plt.cm.RdBu.copy(); cmap_rdbu_map.set_bad('white', alpha=0)\n"
            "# Reversed ploen for Human ET mean (low=warm, high=blue)\n"
            "cmap_ploen_r_map = cmap_ploen_map.reversed()"
        ),
    ])
    print('✓ ce29a1bd: added cmap_rdbu_map and cmap_ploen_r_map')


# ══════════════════════════════════════════════════════════════════════════
# 2. BASEMAP HELPERS (63e90327) — white background, fix east cutoff line
# ══════════════════════════════════════════════════════════════════════════
c = get_cell('63e90327')
if c:
    patch_cell(c, [
        # White background instead of #DCDCDC state fill
        (
            "def _add_basemap(ax, us_color='#DCDCDC'):",
            "def _add_basemap(ax, us_color='white'):"
        ),
        # Fix eastern cutoff line — clip to US boundary
        (
            "    # Eastern OpenET coverage cutoff\n"
            "    ax.plot(_east_x, _east_y, color=color, linewidth=lw, zorder=zorder)",
            "    # Eastern OpenET coverage cutoff — clipped to US boundary\n"
            "    from shapely.geometry import LineString as _SLS\n"
            "    _eline = _SLS(list(zip(_east_x.tolist(), _east_y.tolist())))\n"
            "    _us_geom = _us_dissolved_5070.geometry.iloc[0]\n"
            "    _eclip = _eline.intersection(_us_geom)\n"
            "    if not _eclip.is_empty:\n"
            "        _egeoms = list(_eclip.geoms) if hasattr(_eclip, 'geoms') else [_eclip]\n"
            "        for _eg in _egeoms:\n"
            "            ax.plot(*_eg.xy, color=color, linewidth=lw, zorder=zorder)"
        ),
    ])
    print('✓ 63e90327: white background, clipped east cutoff line')


# ══════════════════════════════════════════════════════════════════════════
# 3. ALL FIGURE CELLS — double font sizes via rcParams
#    Touch every cell that has the rcParams update block
# ══════════════════════════════════════════════════════════════════════════
OLD_RCPARAMS = "plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 11})"
NEW_RCPARAMS = (
    "plt.rcParams.update({\n"
    "    'font.family': 'DejaVu Sans', 'font.size': 22,\n"
    "    'axes.labelsize': 22, 'axes.titlesize': 22,\n"
    "    'xtick.labelsize': 18, 'ytick.labelsize': 18,\n"
    "    'legend.fontsize': 16,\n"
    "})"
)

rcparams_count = 0
for cell in nb['cells']:
    src = ''.join(cell.get('source', []))
    if OLD_RCPARAMS in src:
        patch_cell(cell, [(OLD_RCPARAMS, NEW_RCPARAMS)])
        rcparams_count += 1
print(f'✓ rcParams: updated {rcparams_count} figure cells')


# ══════════════════════════════════════════════════════════════════════════
# 4. FIG 1a — reversed ploen colormap (high ET = blue)
# ══════════════════════════════════════════════════════════════════════════
c = get_cell('e14564eb')
if c:
    patch_cell(c, [
        (
            "cmap=cmap_ploen_map, vmin=0, vmax=_mean_vmax,",
            "cmap=cmap_ploen_r_map, vmin=0, vmax=_mean_vmax,"
        ),
        (
            "cb.set_label('Mean Human ET [mm month\u207b\u00b9]', fontsize=10)",
            "cb.set_label('Mean Human ET [mm month\u207b\u00b9]', fontsize=20)"
        ),
        ("cb.ax.tick_params(labelsize=8)", "cb.ax.tick_params(labelsize=16)"),
    ])
    print('✓ e14564eb: reversed ploen for fig01a, fontsize updated')


# ══════════════════════════════════════════════════════════════════════════
# 5. FIG 1b — RdBu diverging map (positive trend = blue)
# ══════════════════════════════════════════════════════════════════════════
c = get_cell('2dfe2523')
if c:
    patch_cell(c, [
        (
            "cmap=cmap_heatmap0_map, vmin=-_trend_vmax, vmax=_trend_vmax,",
            "cmap=cmap_rdbu_map, vmin=-_trend_vmax, vmax=_trend_vmax,"
        ),
        (
            "cb.set_label('Trend [mm month\u207b\u00b9 yr\u207b\u00b9]', fontsize=10)",
            "cb.set_label('Trend [mm month\u207b\u00b9 yr\u207b\u00b9]', fontsize=20)"
        ),
        ("cb.ax.tick_params(labelsize=8)", "cb.ax.tick_params(labelsize=16)"),
    ])
    print('✓ 2dfe2523: RdBu for fig01b, fontsize updated')


# ══════════════════════════════════════════════════════════════════════════
# 6. FIG 1c — scatter fontsize
# ══════════════════════════════════════════════════════════════════════════
c = get_cell('aab2d65c')
if c:
    patch_cell(c, [
        ("fontsize=10)\nax.set_ylabel", "fontsize=20)\nax.set_ylabel"),
        ("fontsize=10)\nax.legend",     "fontsize=20)\nax.legend"),
        ("fontsize=9, framealpha=0.9, loc='upper right'",
         "fontsize=18, framealpha=0.9, loc='upper right'"),
    ])
    print('✓ aab2d65c: fig01c fontsize updated')


# ══════════════════════════════════════════════════════════════════════════
# 7. FIG 2 slope maps (51e4eb91) — RdBu + darker grey for non-sig
# ══════════════════════════════════════════════════════════════════════════
c = get_cell('51e4eb91')
if c:
    patch_cell(c, [
        # Switch colormap copy to RdBu and set non-sig to darker grey
        (
            "_cmap_s = cmap_heatmap0_map.copy()",
            "_cmap_s = cmap_rdbu_map.copy()\n"
            "    _cmap_s.set_bad('#888888', alpha=1.0)  # non-sig = medium grey"
        ),
        # Legend patch: darker grey
        (
            "facecolor='#DCDCDC', edgecolor='none',\n"
            "                  label='Not significant / < {:d} obs'.format(MIN_OBS_PIXEL))",
            "facecolor='#888888', edgecolor='none',\n"
            "                  label='Not significant / < {:d} obs'.format(MIN_OBS_PIXEL))"
        ),
        # Legend fontsize
        (
            "ax.legend(handles=_leg, loc='lower left', fontsize=7,",
            "ax.legend(handles=_leg, loc='lower left', fontsize=14,"
        ),
    ])
    print('✓ 51e4eb91: RdBu + darker grey non-sig for fig02 slope maps')


# ══════════════════════════════════════════════════════════════════════════
# 8. FIG 3 line plot (4d40b549) — legend position + fontsize
# ══════════════════════════════════════════════════════════════════════════
c = get_cell('4d40b549')
if c:
    patch_cell(c, [
        (
            "ax.legend(title='Drought severity', fontsize=8, title_fontsize=8,\n"
            "          loc='lower right', framealpha=0.9)",
            "ax.legend(title='Drought severity', fontsize=16, title_fontsize=16,\n"
            "          loc='upper left', framealpha=0.9)"
        ),
        # xlabel/ylabel fontsize
        ("ax.set_xlabel('Human ET [mm month\u207b\u00b9]', fontsize=10)",
         "ax.set_xlabel('Human ET [mm month\u207b\u00b9]', fontsize=22)"),
        ("ax.set_ylabel('Mean SIF z-score  (0 = climatological mean)', fontsize=10)",
         "ax.set_ylabel('Mean SIF z-score  (0 = climatological mean)', fontsize=22)"),
    ])
    print('✓ 4d40b549: fig03 legend moved up, fontsize updated')


# ══════════════════════════════════════════════════════════════════════════
# 9. SECTION 7 multi-index cell (5a8ac2b0) — same changes for all variants
# ══════════════════════════════════════════════════════════════════════════
# Also patch the remaining section 7 cells (8a8a07f5, 615f604d, 3313b89c)
for cid in ['5a8ac2b0', '8a8a07f5', '615f604d', '3313b89c']:
    c = get_cell(cid)
    if c is None:
        continue
    src = ''.join(c.get('source', []))
    subs = []
    # Slope maps: heatmap0 → RdBu + darker grey non-sig
    if 'cmap_heatmap0_map' in src:
        subs.append((
            "cmap=cmap_heatmap0_map, vmin=-_sv_max_e, vmax=_sv_max_e,",
            "cmap=cmap_rdbu_map, vmin=-_sv_max_e, vmax=_sv_max_e,"
        ))
        subs.append((
            "_Patch(facecolor='#DCDCDC', edgecolor='none',",
            "_Patch(facecolor='#888888', edgecolor='none',"
        ))
        subs.append((
            "ax.legend(handles=_leg_e, loc='lower left', fontsize=7,",
            "ax.legend(handles=_leg_e, loc='lower left', fontsize=14,"
        ))
    # Fig03 variant legend
    if "loc='lower right'" in src:
        subs.append((
            "fontsize=8, title_fontsize=8, loc='lower right', framealpha=0.9)",
            "fontsize=16, title_fontsize=16, loc='upper left', framealpha=0.9)"
        ))
    if 'ax.set_xlabel' in src and 'Human ET' in src:
        subs.append((
            "ax.set_xlabel('Human ET [mm month\u207b\u00b9]', fontsize=10)",
            "ax.set_xlabel('Human ET [mm month\u207b\u00b9]', fontsize=22)"
        ))
        subs.append((
            "ax.set_ylabel('Mean SIF z-score  (0 = climatological mean)', fontsize=10)",
            "ax.set_ylabel('Mean SIF z-score  (0 = climatological mean)', fontsize=22)"
        ))
    if subs:
        patch_cell(c, subs)
        print(f'✓ {cid}: section 7 variant patched')


# ══════════════════════════════════════════════════════════════════════════
# 10. Panel labels (a)(b)(c) — double fontsize=14 → 28
#     These appear in multiple cells (fig01a, fig01b, fig01c)
# ══════════════════════════════════════════════════════════════════════════
panel_label_old = "fontsize=14, fontweight='bold', va='top', ha='left')"
panel_label_new = "fontsize=28, fontweight='bold', va='top', ha='left')"
panel_count = 0
for cell in nb['cells']:
    src = ''.join(cell.get('source', []))
    if panel_label_old in src:
        patch_cell(cell, [(panel_label_old, panel_label_new)])
        panel_count += 1
print(f'✓ panel labels: updated fontsize in {panel_count} cells')


# ══════════════════════════════════════════════════════════════════════════
# Write patched notebook
# ══════════════════════════════════════════════════════════════════════════
NB_PATH.write_text(json.dumps(nb, indent=1, ensure_ascii=False))
print(f'\nDone. Saved to {NB_PATH}')
print('\nCells to re-execute (in order):')
print('  ce29a1bd  — palette (run first)')
print('  63e90327  — basemap helpers')
print('  e14564eb  — Fig 1a')
print('  2dfe2523  — Fig 1b')
print('  aab2d65c  — Fig 1c scatter')
print('  51e4eb91  — Fig 2 slope maps')
print('  4d40b549  — Fig 3 line plot')
print('  5a8ac2b0+ — Section 7 multi-index (if using spei30d/rzsm_z)')
