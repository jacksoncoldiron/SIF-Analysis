#!/usr/bin/env python3
"""
Figure fix, round 6 (2026-08-20) — CORRECTNESS, not cosmetics.

fig02's Mild and Moderate slope maps have been rendering blank.

The per-category regression cell (5fb71653) stores its results under keys that
embed the threshold text:

    'Mild\\n(-1.0<SPEI<=-0.5)'
    'Moderate\\n(-1.5<SPEI<=-1.0)'

The figure cell (51e4eb91) re-typed those keys by hand and dropped a minus sign
in each:

    'Mild\\n(-1.0<SPEI<=0.5)'        <- missing '-'
    'Moderate\\n(-1.5<SPEI<=1.0)'    <- missing '-'

`pix_slopes_by_cat.get(cat_key, pd.DataFrame())` then returned an empty frame
instead of raising, so both maps drew as all-grey with "n=0 sig. pixels" while
1,395 (Mild) and 688 (Moderate) significant pixels sat unused in the dict.
No Drought and Severe happened to be typed correctly, which is why only two of
the four panels looked wrong.

Fix: derive the lookup keys from `_CAT_DEFS_FIG2` — the same list the
regression cell builds the dict from — so the two can never drift again, and
assert every key resolves.

Run:
    /home/jcoldiron/miniforge3/envs/sif/bin/python \
        src/scripts/patch_viz_notebook_v6.py
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
NB = PROJECT_ROOT / 'src' / 'notebooks' / 'conus' / '03_visualizations_conus.ipynb'

OLD = """_CAT_LABELS = [
    ('No Drought\\n(SPEI>-0.5)',         'SPEI > −0.5',       'fig02_slope_no_drought'),
    ('Mild\\n(-1.0<SPEI≤0.5)',   '−1.0 < SPEI ≤ −0.5', 'fig02_slope_mild'),
    ('Moderate\\n(-1.5<SPEI≤1.0)', '−1.5 < SPEI ≤ −1.0', 'fig02_slope_moderate'),
    ('Severe\\n(SPEI≤-1.5)',         'SPEI ≤ −1.5',   'fig02_slope_severe'),
]"""

NEW = """# Display title + output filename, in the same order as _CAT_DEFS_FIG2.
# The dict KEY is taken from _CAT_DEFS_FIG2 rather than re-typed here: an
# earlier version hand-copied the keys and dropped a minus sign in the Mild and
# Moderate labels, so pix_slopes_by_cat.get() silently returned empty frames
# and both panels rendered blank despite having 1,395 and 688 significant
# pixels. Deriving the keys removes that failure mode entirely.
_CAT_DISPLAY = [
    ('SPEI > −0.5',              'fig02_slope_no_drought'),
    ('−1.0 < SPEI ≤ −0.5', 'fig02_slope_mild'),
    ('−1.5 < SPEI ≤ −1.0', 'fig02_slope_moderate'),
    ('SPEI ≤ −1.5',            'fig02_slope_severe'),
]
_CAT_LABELS = [
    (_defn[0], _title, _fname)
    for _defn, (_title, _fname) in zip(_CAT_DEFS_FIG2, _CAT_DISPLAY)
]

_missing = [k for k, _, _ in _CAT_LABELS if k not in pix_slopes_by_cat]
assert not _missing, 'category keys absent from pix_slopes_by_cat: {}'.format(_missing)
print('Figure 2 categories resolved:')
for _k, _t, _f in _CAT_LABELS:
    print('  {:22s} -> {:,} sig. pixels'.format(_t, len(pix_slopes_by_cat[_k])))"""


def main():
    nb = json.loads(NB.read_text())
    cell = next((c for c in nb['cells'] if c.get('id') == '51e4eb91'), None)
    if cell is None:
        print('cell 51e4eb91 not found')
        sys.exit(1)

    src = ''.join(cell['source'])
    if '_CAT_DISPLAY' in src:
        print('already applied')
        return
    if OLD not in src:
        print('PATTERN NOT FOUND — notebook may have changed')
        sys.exit(1)

    cell['source'] = src.replace(OLD, NEW, 1).splitlines(keepends=True)
    cell['outputs'] = []
    cell['execution_count'] = None
    NB.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + '\n')
    print('Patched 51e4eb91: fig02 category keys derived from _CAT_DEFS_FIG2')


if __name__ == '__main__':
    main()
