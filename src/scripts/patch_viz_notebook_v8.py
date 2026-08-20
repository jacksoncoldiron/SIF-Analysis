#!/usr/bin/env python3
"""
Figure formatting fix, round 8 (2026-08-20).

Panel labels on the maps are drawn with ax.text, which defaults to zorder 3.
State strokes sit at zorder 5, country borders at 6, and the study boundary at
10 — so all of them paint OVER the label and its white backing box. The v7
opaque bbox therefore did not fully solve the collision: the backing is there,
but the Vancouver Island / BC coastline is stroked across it.

Fix: give every panel label zorder=15, above the topmost map layer.

NOT auto-executed. Applying this stages the change; the figures only pick it
up on the next notebook run, which takes ~70 minutes. Batch it with any other
figure feedback rather than running it on its own.

Run:
    /home/jcoldiron/miniforge3/envs/sif/bin/python \
        src/scripts/patch_viz_notebook_v8.py
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
NB = PROJECT_ROOT / 'src' / 'notebooks' / 'conus' / '03_visualizations_conus.ipynb'

EDITS = [
    (
        '6ef66d19',
        """            fontsize=28, fontweight='bold', va='top', ha='left',
            bbox=dict(boxstyle='round,pad=0.25', facecolor='white',
                      edgecolor='none', alpha=1.0))""",
        """            fontsize=28, fontweight='bold', va='top', ha='left', zorder=15,
            bbox=dict(boxstyle='round,pad=0.25', facecolor='white',
                      edgecolor='none', alpha=1.0))""",
        'fig01a: panel label above the border strokes',
    ),
    (
        '7ed929df',
        """            fontsize=28, fontweight='bold', va='top', ha='left',
            bbox=dict(boxstyle='round,pad=0.25', facecolor='white',
                      edgecolor='none', alpha=1.0))""",
        """            fontsize=28, fontweight='bold', va='top', ha='left', zorder=15,
            bbox=dict(boxstyle='round,pad=0.25', facecolor='white',
                      edgecolor='none', alpha=1.0))""",
        'fig01b: panel label above the border strokes',
    ),
    (
        '51e4eb91',
        """            fontsize=20, fontweight='bold', va='top', ha='left',
            bbox=dict(boxstyle='round,pad=0.25', facecolor='white', alpha=1.0,
                      edgecolor='none'))""",
        """            fontsize=20, fontweight='bold', va='top', ha='left', zorder=15,
            bbox=dict(boxstyle='round,pad=0.25', facecolor='white', alpha=1.0,
                      edgecolor='none'))""",
        'fig02: panel label above the border strokes',
    ),
    (
        'fig02b_cell',
        """            fontsize=20, fontweight='bold', va='top', ha='left',
            bbox=dict(boxstyle='round,pad=0.25', facecolor='white', alpha=1.0,
                      edgecolor='none'))""",
        """            fontsize=20, fontweight='bold', va='top', ha='left', zorder=15,
            bbox=dict(boxstyle='round,pad=0.25', facecolor='white', alpha=1.0,
                      edgecolor='none'))""",
        'fig02b: panel label above the border strokes',
    ),
    # The fig02 "not significant" legend also sits under the Baja coastline.
    (
        '51e4eb91',
        """    ax.legend(handles=_leg, loc='lower left', fontsize=14,
              framealpha=0.9, handlelength=1.2, borderpad=0.5)""",
        """    _lg = ax.legend(handles=_leg, loc='lower left', fontsize=14,
                    framealpha=1.0, handlelength=1.2, borderpad=0.5)
    _lg.set_zorder(15)""",
        'fig02: legend above the border strokes',
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
    print('Applied {} edit(s) — STAGED ONLY, not executed.'.format(n))


if __name__ == '__main__':
    main()
