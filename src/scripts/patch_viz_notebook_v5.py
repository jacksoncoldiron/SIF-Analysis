#!/usr/bin/env python3
"""
Figure fixes, round 5 (2026-08-20).

  1. fig02's grey "not significant" fill flooded the whole map.
     `_cmap_s.set_bad('#888888')` colours EVERY NaN in the 189x325 array, and
     every non-cropland cell is NaN too — so the entire rectangle rendered
     grey, not just the cropland pixels that failed significance. It was
     hidden before only because the grey used to be a near-white #DCDCDC.
     Fix: draw grey as its own layer masked to cropland-and-not-significant,
     and let the slope colormap stay transparent on NaN.

  2. fig02's colourbar label ran off the right edge of the figure.
     Fix: shorter label, slightly smaller type.

  3. The multi-index slope maps (cell 615f604d) still carried the old tiny
     colourbar type (label 9pt, ticks 7pt) while every other map uses 17/15.
     Fix: match them, and match the study-boundary stroke width.

Run:
    /home/jcoldiron/miniforge3/envs/sif/bin/python \
        src/scripts/patch_viz_notebook_v5.py
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
NB = PROJECT_ROOT / 'src' / 'notebooks' / 'conus' / '03_visualizations_conus.ipynb'


EDITS = [
    # ── 1. fig02: grey only where cropland AND not significant ───────────
    (
        '51e4eb91',
        """    _slope_plot = np.where(_use_final_mask, _slope_grid, np.nan)

    _cmap_s = cmap_ploen_r_map.copy()
    _cmap_s.set_bad('#888888', alpha=1.0)  # non-sig = medium grey

    fig, ax = _map_fig()

    im = ax.pcolormesh(
        _X5070, _Y5070, _slope_plot,
        cmap=_cmap_s, vmin=-_sv_max, vmax=_sv_max,
        shading='nearest', rasterized=True, zorder=2,
    )""",
        """    # Three states to distinguish, so two layers are needed:
    #   significant slope        -> colour ramp
    #   cropland, not significant -> grey
    #   everything else           -> white (basemap shows through)
    # A single layer with set_bad('#888888') cannot do this: every
    # non-cropland cell in the array is NaN too, so the grey floods the whole
    # 189x325 rectangle.
    _sig_plot = np.where(_use_final_mask & np.isfinite(_slope_grid),
                         _slope_grid, np.nan)
    _nonsig_mask = _use_final_mask & ~np.isfinite(_slope_grid)

    _cmap_s = cmap_ploen_r_map.copy()
    _cmap_s.set_bad('white', alpha=0.0)

    fig, ax = _map_fig()

    # Grey layer first, then the significant slopes on top.
    ax.pcolormesh(
        _X5070, _Y5070,
        np.ma.masked_where(~_nonsig_mask, np.zeros_like(_slope_grid)),
        cmap=ListedColormap(['#888888']), vmin=0, vmax=1,
        shading='nearest', rasterized=True, zorder=2,
    )
    im = ax.pcolormesh(
        _X5070, _Y5070, _sig_plot,
        cmap=_cmap_s, vmin=-_sv_max, vmax=_sv_max,
        shading='nearest', rasterized=True, zorder=3,
    )""",
        'fig02: grey marks only non-significant cropland, not the whole map',
    ),
    (
        '51e4eb91',
        """from matplotlib.patches import Patch""",
        """from matplotlib.patches import Patch
from matplotlib.colors import ListedColormap""",
        'fig02: import ListedColormap for the grey layer',
    ),
    (
        '51e4eb91',
        """    cb.set_label('Slope: SIF z-score per mm month⁻¹ Human ET',
                 fontsize=17)
    cb.ax.tick_params(labelsize=15)""",
        """    # Shorter label: the full wording overflowed the figure width.
    cb.set_label('SIF z-score per mm month⁻¹ Human ET', fontsize=15)
    cb.ax.tick_params(labelsize=14)""",
        'fig02: colourbar label no longer clipped',
    ),

    # ── 3. multi-index slope maps: match the type scale ──────────────────
    # NOTE: this cell stores its label with literal backslash-u escape
    # sequences in the source text rather than decoded characters, so the
    # pattern deliberately avoids the superscript and matches only the sizes.
    (
        '615f604d',
        """', fontsize=9)
            cb.ax.tick_params(labelsize=7)""",
        """', fontsize=15)
            cb.ax.tick_params(labelsize=14)""",
        'multi-index maps: colourbar type matched to the other figures',
    ),
    (
        '615f604d',
        """            _draw_study_boundary(ax, lw=1.8, zorder=10)""",
        """            _draw_study_boundary(ax, lw=2.0, zorder=10)""",
        'multi-index maps: boundary stroke matched to the other figures',
    ),
]


def main():
    nb = json.loads(NB.read_text())
    by_id = {c.get('id'): c for c in nb['cells']}

    n_applied = 0
    for cell_id, old, new, desc in EDITS:
        cell = by_id.get(cell_id)
        if cell is None:
            print('  MISSING cell {} — skipped ({})'.format(cell_id, desc))
            continue
        src = ''.join(cell['source'])
        if new in src:
            print('  already applied: {}'.format(desc))
            continue
        if old not in src:
            print('  PATTERN NOT FOUND in {}: {}'.format(cell_id, desc))
            sys.exit(1)
        cell['source'] = src.replace(old, new, 1).splitlines(keepends=True)
        cell['outputs'] = []
        cell['execution_count'] = None
        n_applied += 1
        print('  {}  ({})'.format(cell_id, desc))

    NB.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + '\n')
    print()
    print('Applied {} edit(s) to {}'.format(n_applied, NB.name))


if __name__ == '__main__':
    main()
