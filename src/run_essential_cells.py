#!/usr/bin/env python3
"""
Script to automatically run essential cells from sif_EDA.ipynb
Run this after a kernel crash to quickly restore the environment.

Usage:
    python run_essential_cells.py
"""

import json
import sys
from pathlib import Path
from IPython import get_ipython
from IPython.core.interactiveshell import InteractiveShell

# Check if we're in a Jupyter environment
try:
    ipython = get_ipython()
    if ipython is None:
        print("❌ This script must be run in a Jupyter/IPython environment.")
        print("   Please run it from a Jupyter notebook cell or IPython session.")
        sys.exit(1)
except:
    print("❌ This script must be run in a Jupyter/IPython environment.")
    print("   Please run it from a Jupyter notebook cell or IPython session.")
    sys.exit(1)

# Path to the notebook
notebook_path = Path("sif_EDA.ipynb")
if not notebook_path.exists():
    notebook_path = Path(__file__).parent / "sif_EDA.ipynb"
    if not notebook_path.exists():
        print(f"❌ Could not find sif_EDA.ipynb")
        print(f"   Looked in: {Path.cwd()} and {Path(__file__).parent}")
        sys.exit(1)

print(f"📓 Loading notebook: {notebook_path}")
print("=" * 70)

# Load the notebook
with open(notebook_path, 'r') as f:
    nb = json.load(f)

cells = nb['cells']

# Define essential cells to run (in order)
essential_cells = [
    (1, "Path verification"),
    (3, "Core imports (xarray, rioxarray, etc.)"),
    (4, "Define paths (data_dir, figures_dir)"),
    (5, "Create file_info catalog (all SIF files)"),
    (9, "Iowa boundary shape (iowa_shape)"),
    (8, "Agricultural masks (ag_masks)"),
    (51, "Drought directory (DROUGHT_DIR)"),
    (61, "compute_sif_climatology() function"),
    (64, "load_sif_da() and apply_agricultural_mask() functions"),
    (66, "_iowa_spatial() function"),
    (77, "load_drought_match_target() function"),
]

print(f"\n🚀 Running {len(essential_cells)} essential cells...\n")

# Track success/failure
successful = []
failed = []

for cell_num, description in essential_cells:
    if cell_num >= len(cells):
        print(f"⚠️  Cell {cell_num} does not exist in notebook (skipping)")
        continue
    
    cell = cells[cell_num]
    if cell.get('cell_type') != 'code':
        print(f"⚠️  Cell {cell_num} is not a code cell (skipping)")
        continue
    
    print(f"▶️  Cell {cell_num}: {description}")
    
    try:
        # Get the source code
        source = ''.join(cell.get('source', []))
        
        if not source.strip():
            print(f"   ✓ Empty cell (skipped)")
            successful.append(cell_num)
            continue
        
        # Execute the cell
        result = ipython.run_cell(source, store_history=False)
        
        if result.success:
            print(f"   ✓ Success")
            successful.append(cell_num)
        else:
            print(f"   ❌ Error: {result.error_in_exec}")
            failed.append((cell_num, result.error_in_exec))
            # Ask if we should continue
            response = input(f"   Continue with remaining cells? (y/n): ").strip().lower()
            if response != 'y':
                print("\n⏹️  Stopped by user")
                break
    
    except Exception as e:
        print(f"   ❌ Exception: {e}")
        failed.append((cell_num, str(e)))
        # Ask if we should continue
        response = input(f"   Continue with remaining cells? (y/n): ").strip().lower()
        if response != 'y':
            print("\n⏹️  Stopped by user")
            break
    
    print()

# Summary
print("=" * 70)
print("📊 SUMMARY")
print("=" * 70)
print(f"✓ Successful: {len(successful)}/{len(essential_cells)}")
print(f"❌ Failed: {len(failed)}/{len(essential_cells)}")

if successful:
    print(f"\n✓ Successfully ran cells: {successful}")

if failed:
    print(f"\n❌ Failed cells:")
    for cell_num, error in failed:
        print(f"   Cell {cell_num}: {error}")

if len(failed) == 0:
    print("\n🎉 All essential cells ran successfully!")
    print("   You can now run your analysis cells.")
else:
    print("\n⚠️  Some cells failed. Please check the errors above.")
    print("   You may need to run failed cells manually.")

