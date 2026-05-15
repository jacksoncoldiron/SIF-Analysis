#!/usr/bin/env python3
"""
MODIS Monthly NDVI Download for Full CONUS (2015-2024)
Runs as batch job on GRIT HPC

Downloads MODIS Terra MOD13A3 monthly NDVI from Google Earth Engine and saves
GeoTIFFs directly to disk aligned to the NLDAS 0.125° CONUS grid via
computePixels(). This script extends the Iowa NDVI download pattern
(07_download_ndvi_iowa.py) to the full contiguous United States.

PRODUCT:
--------
MOD13A3.061 — MODIS Terra Vegetation Indices (1km, Monthly)
GEE Asset: MODIS/061/MOD13A3
Band: 'NDVI' (stored as integer * 10000; divide by 10000 for true NDVI)
Native resolution: 1 km
Temporal coverage: 2000-02 → present

ALIGNMENT:
----------
Output GeoTIFFs are written at the NLDAS 0.125° CONUS grid (hardcoded).
This ensures pixel-perfect alignment with NLDAS, OpenET, SIF, and drought
data in the analysis notebooks.

CONUS GRID MATH:
----------------
NLDAS grid spans 25.0625°–52.9375°N, 124.9375°–67.0625°W at 0.125° spacing
→ 464 cols × 224 rows. Upper-left corner at (-125.0, 53.0).

FILE SIZE:
----------
Each CONUS file is ~415 KB (464×224 float32). 120 files total ≈ 50 MB.

NDVI INTERPRETATION:
--------------------
  NDVI < 0.1   : Bare soil / water / non-vegetated
  0.1–0.3      : Sparse vegetation (possible fallow / early emergence)
  0.3–0.5      : Moderate vegetation density
  0.5–0.8      : Dense, actively growing crops (peak growing season)
  > 0.8        : Very dense canopy (rare for row crops, more common in forest)

Output: SIF-Analysis/data/raw/ndvi/NDVI_CONUS_YYYYMM.tif
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

# ── Study period ──────────────────────────────────────────────────────────────
YEARS = list(range(2015, 2025))   # 2015–2024 (10-year study period)

# ── MODIS MOD13A3 collection ──────────────────────────────────────────────────
MODIS_MONTHLY = 'MODIS/061/MOD13A3'
NDVI_BAND     = 'NDVI'                # integer * 10000; divide to get true NDVI
NDVI_SCALE    = 0.0001                # scale factor to convert to true NDVI (0–1)

# ── CONUS NLDAS 0.125° grid (hardcoded — no reference asset needed) ───────────
# NLDAS grid spans 25.0625°–52.9375°N, 124.9375°–67.0625°W at 0.125° spacing
# → 464 cols × 224 rows. Upper-left corner at (-125.0, 53.0).
TARGET_CRS       = 'EPSG:4326'
TARGET_TRANSFORM = [0.125, 0, -125.0, 0, -0.125, 53.0]
TARGET_WIDTH, TARGET_HEIGHT = 464, 224

# ── GEE Cloud project ─────────────────────────────────────────────────────────
GEE_PROJECT = 'et-research-489120'

# ── Output directory ──────────────────────────────────────────────────────────
# Resolved relative to this script's location so the path is portable across
# machines (HPC, local). Iowa files share the same directory with a different
# filename prefix (NDVI_Iowa_YYYYMM.tif vs NDVI_CONUS_YYYYMM.tif).
OUTPUT_DIR = Path(__file__).resolve().parents[3] / 'data' / 'raw' / 'ndvi'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Log file ──────────────────────────────────────────────────────────────────
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
            # First day of next month as exclusive end date for filterDate()
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
existing = {f.stem for f in OUTPUT_DIR.glob('NDVI_CONUS_*.tif')}

log(f"Output directory:    {OUTPUT_DIR}")
log(f"Total periods:       {len(periods)} (12 months x {len(YEARS)} years)")
log(f"Already downloaded:  {len(existing)} — will skip these")
log(f"Remaining to fetch:  {len(periods) - len(existing)}")
log("")

collection = ee.ImageCollection(MODIS_MONTHLY)
failed     = []

for i, period in enumerate(periods):
    filename = f"NDVI_CONUS_{period['label']}"
    out_path = OUTPUT_DIR / f"{filename}.tif"

    if filename in existing:
        log(f"[{i+1:3d}/{len(periods)}] Skipped (exists): {filename}")
        continue

    try:
        # MOD13A3 is a monthly composite — one image per month.
        # .mean() handles any overlap from edge of tile coverage.
        # Apply scale factor to convert from integer storage to true NDVI (0–1).
        image = (collection
                 .filterDate(period['start'], period['end_excl'])
                 .filterBounds(conus)
                 .select(NDVI_BAND)
                 .mean()
                 .multiply(NDVI_SCALE))   # converts integer*10000 → float NDVI

        # computePixels() exports at an exact affine grid, matching the NLDAS
        # reference used for OpenET and ET-delta data in this project.
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

        # ── Sanity check: verify NDVI values are in plausible range ───────────
        # MODIS NDVI should be between -1 and 1 after scaling; outside this range
        # suggests a scale-factor or fill-value issue.
        try:
            import io as _io
            import numpy as _np
            import rasterio as _rio
            with _rio.open(_io.BytesIO(raw_bytes)) as _src:
                _data = _src.read(1).astype(float)
                _nd   = _src.nodata
                if _nd is not None:
                    _data[_data == _nd] = _np.nan
            _valid = _data[~_np.isnan(_data)]
            if len(_valid) > 0:
                _vmin, _vmax = float(_np.nanmin(_valid)), float(_np.nanmax(_valid))
                if _vmin < -1.5 or _vmax > 1.5:
                    log(f"  WARNING: NDVI range [{_vmin:.3f}, {_vmax:.3f}] — "
                        f"check scale factor for {filename}")
                else:
                    log(f"  NDVI range: [{_vmin:.3f}, {_vmax:.3f}] ok")
        except ImportError:
            pass  # rasterio not available; skip validation

        with open(out_path, 'wb') as f:
            f.write(raw_bytes)

        log(f"[{i+1:3d}/{len(periods)}] Saved: {out_path.name}")

    except Exception as e:
        log(f"[{i+1:3d}/{len(periods)}] FAILED: {filename} — {e}")
        failed.append(period['label'])
        time.sleep(2)   # brief pause before next attempt to avoid rate limits

# =============================================================================
# Summary
# =============================================================================
log("")
log("=" * 60)
log("NDVI CONUS DOWNLOAD COMPLETE")
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
