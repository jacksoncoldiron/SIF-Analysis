#!/usr/bin/env python3
"""
OpenET Monthly ET Download for Iowa (2019-2023)
Runs as batch job on GRIT HPC

Downloads OpenET ENSEMBLE monthly ET from Google Earth Engine and saves
GeoTIFFs directly to disk aligned to the SIF OCO-2 0.05° grid.

Output: SIF-Analysis/data/raw/OpenET/OpenET_Iowa_YYYYMM.tif
"""

import sys
import subprocess
import io
import zipfile
import datetime
import time
from pathlib import Path

# =============================================================================
# Setup: ensure earthengine-api and requests are importable
# =============================================================================
target_dir = str(Path.home() / '.local/lib/python3.12/site-packages')
if target_dir not in sys.path:
    sys.path.insert(0, target_dir)

try:
    import ee
    import requests
    print(f"earthengine-api version: {ee.__version__}", flush=True)
except ImportError:
    print("Installing earthengine-api and requests...", flush=True)
    subprocess.check_call([
        sys.executable, '-m', 'pip', 'install',
        '--target=' + target_dir,
        'earthengine-api', 'requests'
    ])
    import ee
    import requests

# =============================================================================
# Configuration
# =============================================================================

# ── Years to process ──────────────────────────────────────────────────────
YEARS = list(range(2019, 2024))  # 2019–2023

# ── GEE collection ────────────────────────────────────────────────────────
OPENET_MONTHLY = 'OpenET/ENSEMBLE/CONUS/GRIDMET/MONTHLY/v2_0'
ET_BAND        = 'et_ensemble_mad'  # mm/month

# ── Target grid: SIF OCO-2 0.05° ─────────────────────────────────────────
# All outputs aligned to the global SIF grid so pixels snap exactly to
# SIF, ECOSTRESS (clipped/), and NLDAS outputs.
# crsTransform format: [xScale, xShear, xOrigin, yShear, yScale, yOrigin]
TARGET_CRS       = 'EPSG:4326'
TARGET_TRANSFORM = [0.05, 0, -180.0, 0, -0.05, 90.0]

# ── GEE Cloud project ─────────────────────────────────────────────────────
GEE_PROJECT = 'et-research-489120'

# ── Output directory ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
OUTPUT_DIR   = PROJECT_ROOT / 'data' / 'raw' / 'OpenET'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Log file ──────────────────────────────────────────────────────────────
log_file = OUTPUT_DIR / f"download_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"


def log(message):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp}] {message}"
    print(line, flush=True)
    with open(log_file, 'a') as f:
        f.write(line + '\n')


# =============================================================================
# Authenticate and initialize GEE
# =============================================================================
log("Initializing Google Earth Engine...")
try:
    ee.Initialize(project=GEE_PROJECT)
    log("GEE initialized successfully.")
except Exception as e:
    log(f"ERROR: GEE initialization failed: {e}")
    log("Make sure GEE credentials exist at ~/.config/earthengine/credentials")
    log("Run: python3 -c \"import ee; ee.Authenticate(auth_mode='notebook')\"")
    sys.exit(1)

# =============================================================================
# Study area: Iowa
# =============================================================================
iowa = (ee.FeatureCollection('TIGER/2018/States')
          .filter(ee.Filter.eq('NAME', 'Iowa'))
          .geometry())

# =============================================================================
# Monthly period generator
# =============================================================================
def get_monthly_periods(years):
    """Return list of dicts for each month in the given years."""
    periods = []
    for year in years:
        for month in range(1, 13):
            start = datetime.date(year, month, 1)
            # First day of next month as exclusive end date
            if month == 12:
                end_excl = datetime.date(year + 1, 1, 1)
            else:
                end_excl = datetime.date(year, month + 1, 1)
            periods.append({
                'start'   : start.strftime('%Y-%m-%d'),
                'end_excl': end_excl.strftime('%Y-%m-%d'),
                'label'   : start.strftime('%Y%m'),
            })
    return periods


# =============================================================================
# Download loop
# =============================================================================
periods  = get_monthly_periods(YEARS)
existing = {f.stem for f in OUTPUT_DIR.glob('OpenET_Iowa_*.tif')}

log(f"Output directory: {OUTPUT_DIR}")
log(f"Total periods: {len(periods)}")
log(f"Already downloaded: {len(existing)} — will skip these")
log("")

collection = ee.ImageCollection(OPENET_MONTHLY)
failed     = []

for i, period in enumerate(periods):
    filename = f"OpenET_Iowa_{period['label']}"
    out_path = OUTPUT_DIR / f"{filename}.tif"

    if filename in existing:
        log(f"[{i+1:3d}/{len(periods)}] Skipped (exists): {filename}")
        continue

    try:
        image = (collection
                 .filterDate(period['start'], period['end_excl'])
                 .filterBounds(iowa)
                 .select(ET_BAND)
                 .first()
                 .clip(iowa))

        # Get download URL from GEE — aligned to SIF OCO-2 0.05° grid
        url = image.getDownloadURL({
            'region'      : iowa,
            'crs'         : TARGET_CRS,
            'crsTransform': TARGET_TRANSFORM,
            'fileFormat'  : 'GeoTIFF',
        })

        response = requests.get(url, timeout=300)
        response.raise_for_status()

        # GEE returns a zip containing the GeoTIFF — extract it
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            tif_names = [n for n in zf.namelist() if n.endswith('.tif')]
            with open(out_path, 'wb') as f:
                f.write(zf.read(tif_names[0]))

        log(f"[{i+1:3d}/{len(periods)}] Saved: {out_path.name}")

    except Exception as e:
        log(f"[{i+1:3d}/{len(periods)}] FAILED: {filename} — {e}")
        failed.append(period['label'])
        time.sleep(2)

# =============================================================================
# Summary
# =============================================================================
log("")
log("=" * 60)
log("DOWNLOAD COMPLETE")
log("=" * 60)
log(f"Total periods:    {len(periods)}")
log(f"Downloaded:       {len(periods) - len(existing) - len(failed)}")
log(f"Skipped (exists): {len(existing)}")
log(f"Failed:           {len(failed)}")
log(f"Output:           {OUTPUT_DIR}")
log(f"Log:              {log_file}")

if failed:
    log("")
    log("Failed periods (re-run script to retry):")
    for label in failed:
        log(f"  {label}")
