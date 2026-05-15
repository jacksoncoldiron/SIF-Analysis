#!/usr/bin/env python3
"""
OpenET Monthly ET Download for Full CONUS (2015-2024)
Runs as batch job on GRIT HPC

Downloads OpenET ENSEMBLE monthly ET from Google Earth Engine and saves
GeoTIFFs directly to disk aligned to the NLDAS 0.125° CONUS grid via
computePixels(). This script extends the Iowa download pattern
(04_download_openet_iowa.py) to the full contiguous United States.

NOTE: OpenET v2.0 ensemble coverage starts January 2016. Months in 2015 may
be missing or incomplete; the download loop handles these gracefully by logging
failures without crashing. Check the log file after the run.

CONUS GRID MATH:
----------------
NLDAS grid spans 25.0625°–52.9375°N, 124.9375°–67.0625°W at 0.125° spacing
→ 464 cols × 224 rows. Upper-left corner at (-125.0, 53.0).

FILE SIZE:
----------
Each CONUS file is ~415KB (464×224 float32). 120 files total ≈ 50 MB.

Output: SIF-Analysis/data/raw/openet/OpenET_CONUS_YYYYMM.tif
"""

import sys
import subprocess
import datetime
import time
from pathlib import Path

# =============================================================================
# Setup: ensure earthengine-api is importable
# =============================================================================
target_dir = str(Path.home() / '.local/lib/python3.12/site-packages')
if target_dir not in sys.path:
    sys.path.insert(0, target_dir)

try:
    import ee
    print(f"earthengine-api version: {ee.__version__}", flush=True)
except ImportError:
    print("Installing earthengine-api...", flush=True)
    subprocess.check_call([
        sys.executable, '-m', 'pip', 'install',
        '--target=' + target_dir,
        'earthengine-api'
    ])
    import ee

# =============================================================================
# Configuration
# =============================================================================

# ── Years to process ──────────────────────────────────────────────────────
# 10-year study period matching SIF (OCO-2 from 2014-09), Drought Monitor
# (from 2015-01), and CDL data. Safe to re-run; existing files are skipped.
YEARS = list(range(2015, 2025))  # 2015–2024

# ── GEE collection ────────────────────────────────────────────────────────
OPENET_MONTHLY = 'projects/openet/assets/ensemble/conus/gridmet/monthly/v2_0'
ET_BAND        = 'et_ensemble_mad'  # mm/month

# ── CONUS NLDAS 0.125° grid (hardcoded — no reference asset needed) ───────
# NLDAS grid spans 25.0625°–52.9375°N, 124.9375°–67.0625°W at 0.125° spacing
# → 464 cols × 224 rows. Upper-left corner at (-125.0, 53.0).
TARGET_CRS       = 'EPSG:4326'
TARGET_TRANSFORM = [0.125, 0, -125.0, 0, -0.125, 53.0]
TARGET_WIDTH, TARGET_HEIGHT = 464, 224

# ── GEE Cloud project ─────────────────────────────────────────────────────
GEE_PROJECT = 'et-research-489120'

# ── Output directory ──────────────────────────────────────────────────────
# Resolved relative to this script's location so the path is portable across
# machines (HPC, local). Iowa files share the same directory with a different
# filename prefix (OpenET_Iowa_YYYYMM.tif vs OpenET_CONUS_YYYYMM.tif).
OUTPUT_DIR = Path(__file__).resolve().parents[3] / 'data' / 'raw' / 'openet'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Log file ──────────────────────────────────────────────────────────────
log_file = OUTPUT_DIR / f"download_conus_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"


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

log(f"  CRS:        {TARGET_CRS}")
log(f"  Transform:  {TARGET_TRANSFORM}")
log(f"  Dimensions: {TARGET_WIDTH} x {TARGET_HEIGHT}")

# =============================================================================
# CONUS geometry for collection filtering
# =============================================================================
# All US states except Alaska (STATEFP 02) and Hawaii (STATEFP 15).
# convexHull() keeps filterBounds() fast without multi-polygon overhead.
conus = (ee.FeatureCollection('TIGER/2018/States')
           .filter(ee.Filter.neq('STATEFP', '02'))
           .filter(ee.Filter.neq('STATEFP', '15'))
           .geometry()
           .convexHull(maxError=1000))

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
existing = {f.stem for f in OUTPUT_DIR.glob('OpenET_CONUS_*.tif')}

log(f"Output directory: {OUTPUT_DIR}")
log(f"Total periods: {len(periods)}")
log(f"Already downloaded: {len(existing)} — will skip these")
log("")

collection = ee.ImageCollection(OPENET_MONTHLY)
failed     = []

for i, period in enumerate(periods):
    filename = f"OpenET_CONUS_{period['label']}"
    out_path = OUTPUT_DIR / f"{filename}.tif"

    if filename in existing:
        log(f"[{i+1:3d}/{len(periods)}] Skipped (exists): {filename}")
        continue

    try:
        # .mean() aggregates all tiles covering CONUS for this month.
        # filterBounds() with the convexHull geometry is sufficient to select
        # the relevant OpenET tiles; exact clipping is handled by the grid
        # dimensions in computePixels().
        image = (collection
                 .filterDate(period['start'], period['end_excl'])
                 .filterBounds(conus)
                 .select(ET_BAND)
                 .mean())

        # computePixels() is the reliable way to download at an exact affine
        # grid when the native collection CRS differs from the target CRS.
        # getDownloadURL + crsTransform silently falls back to native scale
        # when reprojecting from UTM (EPSG:32610) to WGS84.
        raw_bytes = ee.data.computePixels({
            'expression': image,
            'fileFormat': 'GEO_TIFF',
            'grid': {
                'crsCode': TARGET_CRS,
                'affineTransform': {
                    'scaleX'    : TARGET_TRANSFORM[0],
                    'shearX'    : TARGET_TRANSFORM[1],
                    'translateX': TARGET_TRANSFORM[2],
                    'shearY'    : TARGET_TRANSFORM[3],
                    'scaleY'    : TARGET_TRANSFORM[4],
                    'translateY': TARGET_TRANSFORM[5],
                },
                'dimensions': {
                    'width' : TARGET_WIDTH,
                    'height': TARGET_HEIGHT,
                },
            },
        })

        # ── Post-process: mask zeros and set proper nodata ─────────────────
        # GEE computePixels fills pixels outside the OpenET collection's
        # footprint with 0 (not NaN). Treat exact 0 as nodata — genuine
        # OpenET ET values are always > 0 (even in winter within coverage).
        # Also validates that enough non-zero pixels are present.
        try:
            import io as _io
            import numpy as _np
            import rasterio as _rio
            from rasterio.io import MemoryFile as _MemFile

            with _rio.open(_io.BytesIO(raw_bytes)) as _src:
                _meta  = _src.meta.copy()
                _data  = _src.read(1).astype(_np.float32)

            # Zero → NaN (GEE fill value outside collection footprint)
            _data[_data == 0] = _np.nan

            _n_total = _data.size
            _n_valid = int(_np.isfinite(_data).sum())
            _pct_valid = _n_valid / _n_total if _n_total else 0
            if _pct_valid < 0.05:
                raise ValueError(
                    f"Spatial coverage check FAILED: only {_n_valid}/{_n_total} "
                    f"pixels ({_pct_valid:.0%}) are non-zero — "
                    f"tile-only export suspected. Skipping save."
                )

            # Write corrected GeoTIFF with nodata=NaN
            _meta.update(dtype='float32', nodata=float('nan'))
            with _MemFile() as _mf:
                with _mf.open(**_meta) as _dst:
                    _dst.write(_data, 1)
                raw_bytes = _mf.read()

        except ImportError:
            pass  # rasterio not available; skip post-processing

        with open(out_path, 'wb') as f:
            f.write(raw_bytes)

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
