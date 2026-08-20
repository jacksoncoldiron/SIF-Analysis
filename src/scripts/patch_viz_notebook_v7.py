#!/usr/bin/env python3
"""
Figure formatting fixes, round 7 (2026-08-20).

  1. fig02b's bivariate key: the two axis labels still overlapped each other at
     the bottom-left corner after v4 removed the duplicate arrows. The real
     cause is that the label strings are longer than the 0.135-wide inset, so
     each one runs past the grid and into the other. Fix: shorter labels and a
     slightly larger inset.

  2. Panel labels ("(a)", "SPEI-180d", threshold text) sit at y=0.97 and still
     clip the Vancouver Island / BC coastline, which pokes into the top-left
     even with the v4 top band. Fix: opaque white bbox behind every panel
     label so it reads cleanly over any geometry.

Run:
    /home/jcoldiron/miniforge3/envs/sif/bin/python \
        src/scripts/patch_viz_notebook_v7.py
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
NB = PROJECT_ROOT / 'src' / 'notebooks' / 'conus' / '03_visualizations_conus.ipynb'

_LABEL_BBOX = ("bbox=dict(boxstyle='round,pad=0.25', facecolor='white',\n"
               "                      edgecolor='none', alpha=1.0)")

EDITS = [
    # ── 1. fig02b key: shorter labels, larger inset ──────────────────────
    (
        'fig02b_cell',
        """                          rect=(0.085, 0.17, 0.135, 0.135)):""",
        """                          rect=(0.085, 0.17, 0.155, 0.155)):""",
        'fig02b: enlarge the bivariate key inset',
    ),
    (
        'fig02b_cell',
        """    _add_bivariate_legend(
        fig, BIV_GRID,
        'Buffering slope →',
        'Drought severity →',
    )""",
        """    # Labels kept short: anything longer overruns the inset and the x- and
    # y-labels collide in the corner.
    _add_bivariate_legend(
        fig, BIV_GRID,
        'Buffering →',
        'Drought →',
    )""",
        'fig02b: shorten key labels so they stop colliding',
    ),

    # ── 2. Opaque white bbox behind panel labels ─────────────────────────
    (
        '6ef66d19',
        """    ax.text(0.015, 0.97, '(a)', transform=ax.transAxes,
            fontsize=28, fontweight='bold', va='top', ha='left')""",
        """    ax.text(0.015, 0.97, '(a)', transform=ax.transAxes,
            fontsize=28, fontweight='bold', va='top', ha='left',
            """ + _LABEL_BBOX + ")",
        'fig01a: opaque label backing',
    ),
    (
        '7ed929df',
        """    ax.text(0.015, 0.97, '(b)', transform=ax.transAxes,
            fontsize=28, fontweight='bold', va='top', ha='left')""",
        """    ax.text(0.015, 0.97, '(b)', transform=ax.transAxes,
            fontsize=28, fontweight='bold', va='top', ha='left',
            """ + _LABEL_BBOX + ")",
        'fig01b: opaque label backing',
    ),
    (
        '51e4eb91',
        """            bbox=dict(boxstyle='round,pad=0.25', facecolor='white', alpha=0.8,
                      edgecolor='none'))""",
        """            bbox=dict(boxstyle='round,pad=0.25', facecolor='white', alpha=1.0,
                      edgecolor='none'))""",
        'fig02: make label backing opaque',
    ),
    (
        'fig02b_cell',
        """            bbox=dict(boxstyle='round,pad=0.25', facecolor='white', alpha=0.8,
                      edgecolor='none'))""",
        """            bbox=dict(boxstyle='round,pad=0.25', facecolor='white', alpha=1.0,
                      edgecolor='none'))""",
        'fig02b: make label backing opaque',
    ),
]


def main():
    nb = json.loads(NB.read_text())
    by_id = {c.get('id'): c for c in nb['cells']}

    n = 0
    for cell_id, old, new, desc in EDITS:
        cell = by_id.get(cell_id)
        if cell is None:
            print('  MISSING cell {} ({})'.format(cell_id, desc))
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
        n += 1
        print('  {}  ({})'.format(cell_id, desc))

    NB.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + '\n')
    print()
    print('Applied {} edit(s)'.format(n))


if __name__ == '__main__':
    main()
