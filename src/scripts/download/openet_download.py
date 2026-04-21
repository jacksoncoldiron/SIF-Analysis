#!/usr/bin/env python3
"""
OpenET Monthly ET Download for Iowa (2019-2023)
Runs as batch job on GRIT HPC

Downloads OpenET ENSEMBLE monthly ET from Google Earth Engine and saves
GeoTIFFs directly to disk aligned to the NLDAS 0.125° Iowa reference grid.

Output: SIF-Analysis/data/raw/OpenET/OpenET_Iowa_YYYYMM.tif
"""

import sys
import subprocess
import io
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
YEARS = list(range(2019, 2024))  # 2019–2023

# ── GEE collection ────────────────────────────────────────────────────────
OPENET_MONTHLY = 'projects/openet/assets/ensemble/conus/gridmet/monthly/v2_0'
ET_BAND        = 'et_ensemble_mad'  # mm/month

# ── Reference grid asset (NLDAS 0.125° Iowa) ─────────────────────────────
# Uploaded GeoTIFF defines the target CRS, pixel size, and alignment.
# Resolution and transform are read dynamically at runtime via .projection().
REF_ASSET_ID = 'projects/et-research-489120/assets/NLDAS_Iowa_reference_grid'

# ── GEE Cloud project ─────────────────────────────────────────────────────
GEE_PROJECT = 'et-research-489120'

# ── Output directory ──────────────────────────────────────────────────────
# Hardcoded to GRIT HPC path — this script is intended to run as a batch job there.
OUTPUT_DIR = Path('/home/pielab-sandbox-jcoldiron/SIF-Analysis/data/raw/OpenET')
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
# Reference grid: read CRS, transform, and region from uploaded asset
# =============================================================================
log("Reading reference grid projection from GEE asset...")
ref_image     = ee.Image(REF_ASSET_ID)
ref_band_info = ref_image.getInfo()['bands'][0]
TARGET_CRS       = ref_band_info['crs']
TARGET_TRANSFORM = ref_band_info['crs_transform']  # [xScale, xShear, xOrig, yShear, yScale, yOrig]
REF_WIDTH        = ref_band_info['dimensions'][0]   # 53
REF_HEIGHT       = ref_band_info['dimensions'][1]   # 25
log(f"  CRS:       {TARGET_CRS}")
log(f"  Transform: {TARGET_TRANSFORM}")
log(f"  Dimensions: {REF_WIDTH} x {REF_HEIGHT}")

# Iowa geometry used only for collection filtering
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
        # .mean() aggregates all tiles covering Iowa for this month.
        # No .clip() here — the explicit grid dimensions cover the full Iowa
        # bounding box; exact Iowa boundary clipping happens in post-processing.
        image = (collection
                 .filterDate(period['start'], period['end_excl'])
                 .filterBounds(iowa)
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
                    'width' : REF_WIDTH,
                    'height': REF_HEIGHT,
                },
            },
        })

        # ── Spatial coverage validation ────────────────────────────────────
        # Check that at least 80% of columns have non-constant values.
        # If a tile-only export sneaks through, col_std collapses to 0 for
        # the edge-filled region — catch it before saving.
        import io as _io
        try:
            import numpy as _np
            import rasterio as _rio
            with _rio.open(_io.BytesIO(raw_bytes)) as _src:
                _data = _src.read(1).astype(float)
                _data[_data == _src.nodata] = _np.nan if _src.nodata is not None else _data[_data == _src.nodata]
            _col_std  = _np.nanstd(_data, axis=0)
            _n_cols   = _col_std.size
            _n_vary   = int((_col_std > 0.01).sum())
            _pct_vary = _n_vary / _n_cols if _n_cols else 0
            if _pct_vary < 0.8:
                raise ValueError(
                    f"Spatial coverage check FAILED: only {_n_vary}/{_n_cols} "
                    f"columns ({_pct_vary:.0%}) have spatially-varying values — "
                    f"tile-only export suspected. Skipping save."
                )
        except ImportError:
            pass  # rasterio not available; skip validation

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
