#!/usr/bin/env python3
"""
MODIS Monthly NDVI Download for Iowa (2015-2024)
Runs as batch job on GRIT HPC

Downloads MODIS Terra MOD13A3 monthly NDVI from Google Earth Engine and saves
GeoTIFFs directly to disk aligned to the NLDAS 0.125° Iowa reference grid.

NDVI is used alongside SIF to:
  - Identify fallow fields (low or near-zero NDVI in growing season)
  - Distinguish active vs dormant crop states
  - Provide a complementary greenness index to SIF for the drought/irrigation analysis

PRODUCT:
--------
MOD13A3.061 — MODIS Terra Vegetation Indices (1km, Monthly)
GEE Asset: MODIS/061/MOD13A3
Band: 'NDVI' (stored as integer * 10000; divide by 10000 for true NDVI)
Native resolution: 1 km
Temporal coverage: 2000-02 → present

ALIGNMENT:
----------
Output GeoTIFFs are reprojected to the NLDAS 0.125° Iowa reference grid using
the same reference asset used for OpenET downloads. This ensures pixel-perfect
alignment with NLDAS, OpenET, SIF, and drought data in the analysis notebooks.

NDVI INTERPRETATION:
--------------------
  NDVI < 0.1   : Bare soil / water / non-vegetated
  0.1–0.3      : Sparse vegetation (possible fallow / early emergence)
  0.3–0.5      : Moderate vegetation density
  0.5–0.8      : Dense, actively growing crops (peak growing season)
  > 0.8        : Very dense canopy (rare for row crops, more common in forest)

Output: SIF-Analysis/data/raw/NDVI/NDVI_Iowa_YYYYMM.tif
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

# ── Study period ─────────────────────────────────────────────────────────────
YEARS = list(range(2015, 2025))   # 2015–2024 (10-year study period)

# ── MODIS MOD13A3 collection ─────────────────────────────────────────────────
MODIS_MONTHLY = 'MODIS/061/MOD13A3'
NDVI_BAND     = 'NDVI'                 # integer * 10000; divide to get true NDVI
NDVI_SCALE    = 0.0001                 # scale factor to convert to true NDVI (0–1)

# ── Reference grid asset (NLDAS 0.125° Iowa) ─────────────────────────────────
# Same asset used for OpenET. Defines target CRS, pixel size, and alignment.
REF_ASSET_ID = 'projects/et-research-489120/assets/NLDAS_Iowa_reference_grid'

# ── GEE Cloud project ─────────────────────────────────────────────────────────
GEE_PROJECT = 'et-research-489120'

# ── Output directory ──────────────────────────────────────────────────────────
OUTPUT_DIR = Path('/home/pielab-sandbox-jcoldiron/SIF-Analysis/data/raw/NDVI')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Log file ──────────────────────────────────────────────────────────────────
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
# Reference grid: read CRS, transform, and dimensions from uploaded asset
# =============================================================================
log("Reading reference grid projection from GEE asset...")
ref_image     = ee.Image(REF_ASSET_ID)
ref_band_info = ref_image.getInfo()['bands'][0]
TARGET_CRS       = ref_band_info['crs']
TARGET_TRANSFORM = ref_band_info['crs_transform']   # [xScale, xShear, xOrig, yShear, yScale, yOrig]
REF_WIDTH        = ref_band_info['dimensions'][0]    # 53 pixels (Iowa lon extent at 0.125°)
REF_HEIGHT       = ref_band_info['dimensions'][1]    # 25 pixels (Iowa lat extent at 0.125°)
log(f"  CRS:        {TARGET_CRS}")
log(f"  Transform:  {TARGET_TRANSFORM}")
log(f"  Dimensions: {REF_WIDTH} x {REF_HEIGHT}")

# Iowa geometry — used only for collection filtering (reduces data transferred)
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
existing = {f.stem for f in OUTPUT_DIR.glob('NDVI_Iowa_*.tif')}

log(f"Output directory:    {OUTPUT_DIR}")
log(f"Total periods:       {len(periods)} (12 months x {len(YEARS)} years)")
log(f"Already downloaded:  {len(existing)} — will skip these")
log(f"Remaining to fetch:  {len(periods) - len(existing)}")
log("")

collection = ee.ImageCollection(MODIS_MONTHLY)
failed     = []

for i, period in enumerate(periods):
    filename = f"NDVI_Iowa_{period['label']}"
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
                 .filterBounds(iowa)
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
                    'width' : REF_WIDTH,
                    'height': REF_HEIGHT,
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
                    log(f"  NDVI range: [{_vmin:.3f}, {_vmax:.3f}] ✓")
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
log("NDVI DOWNLOAD COMPLETE")
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
