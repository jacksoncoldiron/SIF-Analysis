#!/usr/bin/env python3
"""
Figure formatting fixes, round 4 (2026-08-20).

Targeted follow-ups after reviewing the round-3 output:

  1. Map panel labels ("(a)", "SPEI-90d", ...) collided with the Canada border
     and the Washington coastline, which run right along the top of the frame.
     Fix: reserve a white band above the domain by padding the top of the map
     extent, so labels sit clear of any geometry.

  2. fig02b's bivariate key sat half outside the axes and its two axis labels
     overlapped each other in the corner. Fix: reposition the inset and drop
     the duplicate annotate arrows (the labels already carry an arrow glyph).

  3. fig03's legend overlapped the Severe-drought line in the California panel.
     Fix: reserve headroom at the bottom of the axes so the lower-right corner
     is always empty before the legend is drawn.

  4. fig01d's y-axis was stretched to -20..+12 by a handful of outliers,
     compressing the actual cloud into a thin band. Fix: robust symmetric
     limits from the 0.5/99.5 percentiles.

Applies string-level edits to specific cells rather than rewriting them.

Run:
    /home/jcoldiron/miniforge3/envs/sif/bin/python \
        src/scripts/patch_viz_notebook_v4.py
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
NB = PROJECT_ROOT / 'src' / 'notebooks' / 'conus' / '03_visualizations_conus.ipynb'


# (cell_id, old, new, description)
EDITS = [
    # ── 1. Top padding on the map extent ─────────────────────────────────
    (
        '63e90327',
        """_sminx, _sminy, _smaxx, _smaxy = _study_area_geom.bounds
_PAD = 12_000     # 12 km — just enough that the boundary stroke is not clipped
_xmin, _xmax = _sminx - _PAD, _smaxx + _PAD
_ymin, _ymax = _sminy - _PAD, _smaxy + _PAD""",
        """_sminx, _sminy, _smaxx, _smaxy = _study_area_geom.bounds
_PAD = 12_000     # 12 km — just enough that the boundary stroke is not clipped
# Extra band above the domain so panel labels ("(a)", "SPEI-90d", ...) sit on
# white space instead of colliding with the Canada border and the Washington
# coastline, both of which run along the very top of the frame.
_TOP_PAD = 0.085 * (_smaxy - _sminy)
_xmin, _xmax = _sminx - _PAD, _smaxx + _PAD
_ymin, _ymax = _sminy - _PAD, _smaxy + _TOP_PAD""",
        'reserve a white band above the domain for panel labels',
    ),

    # ── 2a. fig02b bivariate key placement ───────────────────────────────
    (
        'fig02b_cell',
        """def _add_bivariate_legend(fig, grid, xlabel, ylabel,
                          rect=(0.055, 0.13, 0.15, 0.15)):""",
        """def _add_bivariate_legend(fig, grid, xlabel, ylabel,
                          rect=(0.085, 0.17, 0.135, 0.135)):""",
        'move bivariate key fully inside the axes',
    ),
    (
        'fig02b_cell',
        """    lax.set_xlabel(xlabel, fontsize=12, labelpad=4)
    lax.set_ylabel(ylabel, fontsize=12, labelpad=4)
    lax.annotate('', xy=(n, -0.25), xytext=(0, -0.25),
                 annotation_clip=False,
                 arrowprops=dict(arrowstyle='->', lw=1.2, color='#333333'))
    lax.annotate('', xy=(-0.25, n), xytext=(-0.25, 0),
                 annotation_clip=False,
                 arrowprops=dict(arrowstyle='->', lw=1.2, color='#333333'))
    return lax""",
        """    # The labels already carry an arrow glyph, so the separate annotate
    # arrows are redundant — and they were what overlapped in the corner.
    lax.set_xlabel(xlabel, fontsize=13, labelpad=6)
    lax.set_ylabel(ylabel, fontsize=13, labelpad=6)
    return lax""",
        'drop duplicate arrows that collided in the key corner',
    ),

    # ── 3. fig03 legend headroom ─────────────────────────────────────────
    (
        '4d40b549',
        """    _y0, _y1 = ax.get_ylim()
    ax.set_ylim(_y0 - 0.03 * (_y1 - _y0), _y1)

    # Legend inside the frame, lower right — compact so it clears the lines""",
        """    # Reserve headroom at the bottom so the lower-right corner is empty
    # before the legend is placed. Without this the Severe-drought line runs
    # straight through the legend box in the California panel.
    _y0, _y1 = ax.get_ylim()
    ax.set_ylim(_y0 - 0.30 * (_y1 - _y0), _y1)

    # Legend inside the frame, lower right — compact so it clears the lines""",
        'reserve bottom headroom so the legend never overlaps a line',
    ),

    # ── 4. fig01d robust y-limits ────────────────────────────────────────
    (
        'fig01d_cell',
        """_report_fits(_fits_d, 'fig01d')

plt.tight_layout()""",
        """_report_fits(_fits_d, 'fig01d')

# Robust symmetric y-limits: a handful of extreme trends stretched the axis to
# -20..+12 and squashed the actual cloud into a thin band.
_ylim_d = float(np.nanpercentile(np.abs(df_sd['trend'].values), 99.5))
ax.set_ylim(-_ylim_d, _ylim_d)

plt.tight_layout()""",
        'robust y-limits so the point cloud is not squashed by outliers',
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
        src = src.replace(old, new, 1)
        cell['source'] = src.splitlines(keepends=True)
        cell['outputs'] = []
        cell['execution_count'] = None
        n_applied += 1
        print('  {}  ({})'.format(cell_id, desc))

    NB.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + '\n')
    print()
    print('Applied {} edit(s) to {}'.format(n_applied, NB.name))


if __name__ == '__main__':
    main()
