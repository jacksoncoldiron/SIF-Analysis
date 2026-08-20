#!/usr/bin/env python3
"""Apply aesthetic changes after the boundary-fix notebook run completes."""
import json
from pathlib import Path

NB_PATH = Path(__file__).resolve().parents[2] / 'src/notebooks/conus/03_visualizations_conus.ipynb'
nb = json.loads(NB_PATH.read_text())
cells_by_id = {c.get('id', ''): c for c in nb['cells']}

def patch(cell, subs):
    src = ''.join(cell['source'])
    for old, new in subs:
        if old in src:
            src = src.replace(old, new)
        else:
            print(f'  WARNING not found: {old[:80]!r}')
    cell['source'] = [line + '\n' for line in src.split('\n')]
    if cell['source'] and cell['source'][-1] == '\n':
        cell['source'][-1] = ''

# 1. rcParams: axes.labelsize/titlesize -15% (22→19), legend.fontsize -25% (16→12)
OLD_RC = ("plt.rcParams.update({\n"
          "    'font.family': 'DejaVu Sans', 'font.size': 22,\n"
          "    'axes.labelsize': 22, 'axes.titlesize': 22,\n"
          "    'xtick.labelsize': 18, 'ytick.labelsize': 18,\n"
          "    'legend.fontsize': 16,\n"
          "})")
NEW_RC = ("plt.rcParams.update({\n"
          "    'font.family': 'DejaVu Sans', 'font.size': 22,\n"
          "    'axes.labelsize': 19, 'axes.titlesize': 19,\n"
          "    'xtick.labelsize': 18, 'ytick.labelsize': 18,\n"
          "    'legend.fontsize': 12,\n"
          "})")
n = 0
for cell in nb['cells']:
    if OLD_RC in ''.join(cell.get('source', [])):
        patch(cell, [(OLD_RC, NEW_RC)])
        n += 1
print(f'✓ rcParams: {n} cells updated')

# 2. fig01a (6ef66d19): reversed ploen + colorbar fontsize
c = cells_by_id.get('6ef66d19')
if c:
    patch(c, [
        ('cmap=cmap_ploen_map, vmin=0, vmax=_mean_vmax,',
         'cmap=cmap_ploen_r_map, vmin=0, vmax=_mean_vmax,'),
        ("cb.set_label('Mean Human ET [mm month\\u207b\\u00b9]', fontsize=10)",
         "cb.set_label('Mean Human ET [mm month\\u207b\\u00b9]', fontsize=17)"),
        ('cb.ax.tick_params(labelsize=8)', 'cb.ax.tick_params(labelsize=15)'),
    ])
    print('✓ 6ef66d19 (fig01a): cmap_ploen_r_map, colorbar fontsize')

# 3. fig01b (7ed929df): RdBu (positive = blue) + colorbar fontsize
c = cells_by_id.get('7ed929df')
if c:
    patch(c, [
        ('cmap=cmap_heatmap0_map, vmin=-_trend_vmax, vmax=_trend_vmax,',
         'cmap=cmap_rdbu_map, vmin=-_trend_vmax, vmax=_trend_vmax,'),
        ("cb.set_label('Trend [mm month\\u207b\\u00b9 yr\\u207b\\u00b9]', fontsize=10)",
         "cb.set_label('Trend [mm month\\u207b\\u00b9 yr\\u207b\\u00b9]', fontsize=17)"),
        ('cb.ax.tick_params(labelsize=8)', 'cb.ax.tick_params(labelsize=15)'),
    ])
    print('✓ 7ed929df (fig01b): cmap_rdbu_map, colorbar fontsize')

# 4. fig02 (51e4eb91): ploen_r colormap + legend centered above
c = cells_by_id.get('51e4eb91')
if c:
    patch(c, [
        ('_cmap_s = cmap_rdbu_map.copy()\n'
         "    _cmap_s.set_bad('#888888', alpha=1.0)  # non-sig = medium grey",
         '_cmap_s = cmap_ploen_r_map.copy()\n'
         "    _cmap_s.set_bad('#888888', alpha=1.0)  # non-sig = medium grey"),
        ("ax.legend(handles=_leg, loc='lower left', fontsize=14,\n"
         "              framealpha=0.85, handlelength=1.2)",
         "ax.legend(handles=_leg, loc='lower center',\n"
         "              bbox_to_anchor=(0.5, 1.02),\n"
         "              fontsize=11, framealpha=0.85, handlelength=1.2)"),
    ])
    print('✓ 51e4eb91 (fig02): ploen_r, legend above center')

# 5. fig03 (4d40b549): ylabel clean, xlabel fontsize, legend outside right
c = cells_by_id.get('4d40b549')
if c:
    patch(c, [
        ("ax.set_ylabel('Mean SIF z-score  (0 = climatological mean)', fontsize=22)",
         "ax.set_ylabel('Mean SIF z-score', fontsize=19)"),
        ("ax.set_xlabel('Human ET [mm month\\u207b\\u00b9]', fontsize=10)",
         "ax.set_xlabel('Human ET [mm month\\u207b\\u00b9]', fontsize=19)"),
        ("ax.legend(title='Drought severity', fontsize=16, title_fontsize=16,\n"
         "          loc='upper left', framealpha=0.9)",
         "ax.legend(title='Drought severity', fontsize=12, title_fontsize=12,\n"
         "          loc='upper left', bbox_to_anchor=(1.01, 1), framealpha=0.9)"),
    ])
    print('✓ 4d40b549 (fig03): ylabel, xlabel, legend outside')

NB_PATH.write_text(json.dumps(nb, indent=1, ensure_ascii=False))
print('\nDone.')
