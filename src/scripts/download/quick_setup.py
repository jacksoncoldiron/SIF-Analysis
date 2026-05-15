"""
Quick Setup Script for sif_EDA.ipynb
====================================

Run this in a Jupyter notebook cell to automatically execute all essential setup cells.

Usage in Jupyter:
    %run quick_setup.py

Or copy this entire file into a notebook cell and run it.
"""

import json
from pathlib import Path

# Get the notebook path
notebook_path = Path("sif_EDA.ipynb")
if not notebook_path.exists():
    notebook_path = Path.cwd() / "sif_EDA.ipynb"

print(f"📓 Loading notebook: {notebook_path}")
print("=" * 70)

# Load the notebook
with open(notebook_path, 'r') as f:
    nb = json.load(f)

cells = nb['cells']

# Essential cells in order
essential_cells = [
    (1, "Path verification"),
    (3, "Core imports"),
    (4, "Define paths"),
    (5, "Create file_info catalog"),
    (9, "Iowa boundary shape"),
    (8, "Agricultural masks"),
    (51, "Drought directory"),
    (61, "compute_sif_climatology()"),
    (64, "load_sif_da() and apply_agricultural_mask()"),
    (66, "_iowa_spatial()"),
    (77, "load_drought_match_target()"),
]

print(f"\n🚀 Running {len(essential_cells)} essential cells...\n")

successful = []
failed = []

for cell_num, description in essential_cells:
    if cell_num >= len(cells):
        print(f"⚠️  Cell {cell_num} not found (skipping)")
        continue
    
    cell = cells[cell_num]
    if cell.get('cell_type') != 'code':
        continue
    
    print(f"▶️  Cell {cell_num}: {description}")
    
    try:
        source = ''.join(cell.get('source', []))
        if source.strip():
            exec(source, globals())
            print(f"   ✓ Success")
            successful.append(cell_num)
        else:
            print(f"   ✓ Empty (skipped)")
            successful.append(cell_num)
    except Exception as e:
        print(f"   ❌ Error: {e}")
        failed.append((cell_num, str(e)))
        print(f"   Continuing...")
    
    print()

print("=" * 70)
print(f"✓ Successful: {len(successful)}/{len(essential_cells)}")
if failed:
    print(f"❌ Failed: {len(failed)}")
    for cell_num, error in failed:
        print(f"   Cell {cell_num}: {error}")
else:
    print("🎉 All cells ran successfully!")

