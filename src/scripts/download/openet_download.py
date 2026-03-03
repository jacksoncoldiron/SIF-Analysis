#!/usr/bin/env python3
"""
OpenET Bimonthly ET Download for Iowa (2019-2023)
Runs as batch job on GRIT HPC

Downloads OpenET ENSEMBLE daily ET from Google Earth Engine, aggregates
to bimonthly medians, and saves GeoTIFFs directly to disk aligned to the
SIF OCO-2 0.05° grid.

Output: SIF-Analysis/data/raw/OpenET/OpenET_Iowa_YYYYMMDD_YYYYMMDD.tif
"""

import sys
import subprocess
import io
import zipfile
import calendar
import datetime
import time
from pathlib import Path

# =============================================================================
# Setup: ensure earthengine-api and requests are importable
# =============================================================================
target_dir = '/home/jcoldiron/.local/lib/python3.12/site-packages'
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
OPENET_DAILY = 'OpenET/ENSEMBLE/CONUS/GRIDMET/DAILY/v2_0'
ET_BAND      = 'et_ensemble_mad'        # mm/day
COUNT_BAND   = 'et_ensemble_mad_count'  # number of models (quality proxy)

# ── Quality filter ────────────────────────────────────────────────────────
MIN_MODEL_COUNT = 3  # require at least 3 of 6 models to agree

# ── Target grid: SIF OCO-2 0.05° ─────────────────────────────────────────
# All outputs aligned to the global SIF grid so pixels snap exactly to
# SIF, ECOSTRESS (clipped/), and NLDAS outputs.
# crsTransform format: [xScale, xShear, xOrigin, yShear, yScale, yOrigin]
TARGET_CRS       = 'EPSG:4326'
TARGET_TRANSFORM = [0.05, 0, -180.0, 0, -0.05, 90.0]

# ── GEE Cloud project ─────────────────────────────────────────────────────
# Replace with your actual GEE Cloud project ID
GEE_PROJECT = 'your-project-id'

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
# Bimonthly period generator
# =============================================================================
def get_bimonthly_periods(years):
    """Return list of dicts for each bimonthly period (1-15, 16-end) per month."""
    periods = []
    for year in years:
        for month in range(1, 13):
            last_day = calendar.monthrange(year, month)[1]

            # First half: 1–15
            s1 = datetime.date(year, month, 1)
            e1 = datetime.date(year, month, 15)
            periods.append({
                'start'   : s1.strftime('%Y-%m-%d'),
                'end'     : e1.strftime('%Y-%m-%d'),
                'end_excl': (e1 + datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                'label'   : f'{s1.strftime("%Y%m%d")}_{e1.strftime("%Y%m%d")}',
            })

            # Second half: 16–end
            s2 = datetime.date(year, month, 16)
            e2 = datetime.date(year, month, last_day)
            periods.append({
                'start'   : s2.strftime('%Y-%m-%d'),
                'end'     : e2.strftime('%Y-%m-%d'),
                'end_excl': (e2 + datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                'label'   : f'{s2.strftime("%Y%m%d")}_{e2.strftime("%Y%m%d")}',
            })
    return periods


# =============================================================================
# Masking
# =============================================================================
def mask_low_quality(image):
    """Mask pixels where fewer than MIN_MODEL_COUNT models contributed."""
    band_names = image.bandNames()
    has_count  = band_names.contains(COUNT_BAND)

    def apply_count_mask(img):
        return img.updateMask(img.select(COUNT_BAND).gte(MIN_MODEL_COUNT))

    return ee.Image(ee.Algorithms.If(has_count, apply_count_mask(image), image))


# =============================================================================
# Process one bimonthly period → median ET image
# =============================================================================
def process_period(period, collection):
    """Compute bimonthly median ET for one period, clipped to Iowa."""
    filtered = (
        collection
        .filterDate(period['start'], period['end_excl'])
        .filterBounds(iowa)
        .map(mask_low_quality)
    )

    median_et = filtered.select(ET_BAND).median().rename(ET_BAND)
    return median_et.clip(iowa).set({
        'period_start': period['start'],
        'period_end'  : period['end'],
        'label'       : period['label'],
    })


# =============================================================================
# Download loop
# =============================================================================
periods  = get_bimonthly_periods(YEARS)
existing = {f.stem for f in OUTPUT_DIR.glob('OpenET_Iowa_*.tif')}

log(f"Output directory: {OUTPUT_DIR}")
log(f"Total periods: {len(periods)}")
log(f"Already downloaded: {len(existing)} — will skip these")
log("")

collection = ee.ImageCollection(OPENET_DAILY)
failed     = []

for i, period in enumerate(periods):
    filename = f"OpenET_Iowa_{period['label']}"
    out_path = OUTPUT_DIR / f"{filename}.tif"

    if filename in existing:
        log(f"[{i+1:3d}/{len(periods)}] Skipped (exists): {filename}")
        continue

    try:
        image = process_period(period, collection)

        # Get download URL from GEE — aligned to SIF OCO-2 0.05° grid
        url = image.select(ET_BAND).getDownloadURL({
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
