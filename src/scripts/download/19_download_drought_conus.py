#!/usr/bin/env python3
"""
Combined Drought Data Download for Full CONUS (2015-2024)
Runs as batch job on GRIT HPC

Downloads two drought datasets from Google Earth Engine and saves GeoTIFFs
directly to disk aligned to the NLDAS 0.125° CONUS grid via computePixels().

==============================================================================
SECTION 1 — US Drought Monitor (USDM)
==============================================================================
Collection : projects/sat-io/open-datasets/us-drought-monitor
Band       : 'DM' (drought category, weekly snapshots)
Period     : 2015-01 through 2024-12, bimonthly
             First half  → USDM_CONUS_YYYY-MM_1.tif  (days  1–15)
             Second half → USDM_CONUS_YYYY-MM_2.tif  (days 16–end of month)
Aggregation: .mean() of all weekly images within each bimonthly window

DM CATEGORY CODES:
  -1  : No drought
   0  : D0 — Abnormally Dry
   1  : D1 — Moderate Drought
   2  : D2 — Severe Drought
   3  : D3 — Extreme Drought
   4  : D4 — Exceptional Drought

==============================================================================
SECTION 2 — GRIDMET Drought Metrics
==============================================================================
Collection : GRIDMET/DROUGHT
            https://developers.google.com/earth-engine/datasets/catalog/GRIDMET_DROUGHT
Period     : 2015-01 through 2024-12, monthly means (all 5-day composites
             within each calendar month averaged together)
Bands      : spi30d, spi90d, spei30d, spei90d, eddi30d, eddi90d, pdsi, z
             (8 bands saved in a single multi-band GeoTIFF per month)
Output     : GRIDMET_drought_YYYYMM.tif

DROUGHT THRESHOLDS (used in analysis notebook to derive drought categories):

  SPI / SPEI (Standardized Precipitation / Precip-Evapotranspiration Index):
    >= 0         : No drought
    -0.5 to  0  : Abnormally Dry (D0)
    -1.0 to -0.5 : Moderate Drought (D1)
    -1.5 to -1.0 : Severe Drought (D2)
    -2.0 to -1.5 : Extreme Drought (D3)
    < -2.0       : Exceptional Drought (D4)

  EDDI (Evaporative Demand Drought Index):
    <= 0         : No drought
     0  to  0.5  : Abnormally Dry (D0)
     0.5 to  1.0 : Moderate Drought (D1)
     1.0 to  1.5 : Severe Drought (D2)
     1.5 to  2.0 : Extreme Drought (D3)
    > 2.0        : Exceptional Drought (D4)

  PDSI (Palmer Drought Severity Index):
    > -1         : No drought
    -1  to -2    : Moderate Drought (D1)
    -2  to -3    : Severe Drought (D2)
    -3  to -4    : Extreme Drought (D3)
    < -4         : Exceptional Drought (D4)

  Z (Palmer Z-index): analogous thresholds to PDSI but for shorter timescales.

==============================================================================
CONUS GRID MATH:
----------------
NLDAS grid spans 25.0625°–52.9375°N, 124.9375°–67.0625°W at 0.125° spacing
→ 464 cols × 224 rows. Upper-left corner at (-125.0, 53.0).

Output dirs:
  SIF-Analysis/data/raw/drought_usdm/
  SIF-Analysis/data/raw/drought_gridmet/
"""

import sys
import subprocess
import calendar
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
# Top-level flags — set False to skip a section
# =============================================================================
DOWNLOAD_USDM    = True   # set False to skip USDM
DOWNLOAD_GRIDMET = True   # set False to skip GRIDMET

# =============================================================================
# Configuration
# =============================================================================

# ── Study period ──────────────────────────────────────────────────────────────
YEARS = list(range(2015, 2025))   # 2015–2024 (10-year study period)

# ── CONUS NLDAS 0.125° grid ───────────────────────────────────────────────────
CONUS_CRS              = 'EPSG:4326'
CONUS_TRANSFORM        = [0.125, 0, -125.0, 0, -0.125, 53.0]
CONUS_WIDTH, CONUS_HEIGHT = 464, 224

# ── GEE Cloud project ─────────────────────────────────────────────────────────
GEE_PROJECT = 'et-research-489120'

# ── USDM collection ───────────────────────────────────────────────────────────
USDM_COLLECTION = 'projects/sat-io/open-datasets/us-drought-monitor'
USDM_BAND       = 'DM'

# ── GRIDMET drought collection ────────────────────────────────────────────────
GRIDMET_COLLECTION = 'GRIDMET/DROUGHT'
GRIDMET_BANDS      = ['spi30d', 'spi90d', 'spei30d', 'spei90d',
                      'eddi30d', 'eddi90d', 'pdsi', 'z']

# ── Output directories ────────────────────────────────────────────────────────
_PROJECT_ROOT   = Path(__file__).resolve().parents[3]
USDM_DIR        = _PROJECT_ROOT / 'data' / 'raw' / 'drought_usdm'
GRIDMET_DIR     = _PROJECT_ROOT / 'data' / 'raw' / 'drought_gridmet'
USDM_DIR.mkdir(parents=True, exist_ok=True)
GRIDMET_DIR.mkdir(parents=True, exist_ok=True)

# ── Log file (shared for both sections) ──────────────────────────────────────
_ts      = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = _PROJECT_ROOT / 'data' / 'raw' / f"download_drought_conus_log_{_ts}.txt"


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

log(f"  CRS:        {CONUS_CRS}")
log(f"  Transform:  {CONUS_TRANSFORM}")
log(f"  Dimensions: {CONUS_WIDTH} x {CONUS_HEIGHT}")
log("")

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
# Shared helper: call computePixels() on the CONUS NLDAS grid
# =============================================================================
def compute_pixels_conus(image):
    """
    Download `image` at the CONUS NLDAS 0.125° grid via computePixels().
    Returns raw bytes (GeoTIFF).
    """
    return ee.data.computePixels({
        'expression': image,
        'fileFormat': 'GEO_TIFF',
        'grid': {
            'crsCode': CONUS_CRS,
            'affineTransform': {
                'scaleX'    : CONUS_TRANSFORM[0],
                'shearX'    : CONUS_TRANSFORM[1],
                'translateX': CONUS_TRANSFORM[2],
                'shearY'    : CONUS_TRANSFORM[3],
                'scaleY'    : CONUS_TRANSFORM[4],
                'translateY': CONUS_TRANSFORM[5],
            },
            'dimensions': {
                'width' : CONUS_WIDTH,
                'height': CONUS_HEIGHT,
            },
        },
    })


# =============================================================================
# Period generators
# =============================================================================
def get_bimonthly_periods(years):
    """
    Return list of dicts for each bimonthly window (first half / second half)
    in the given years. Keys: start, end_excl, label.
    """
    periods = []
    for year in years:
        for month in range(1, 13):
            eom = calendar.monthrange(year, month)[1]   # last day of month
            # First half: days 1–15 (end exclusive: day 16)
            periods.append({
                'start'   : f"{year}-{month:02d}-01",
                'end_excl': f"{year}-{month:02d}-16",
                'label'   : f"{year}-{month:02d}_1",
            })
            # Second half: days 16–EOM (end exclusive: first day of next month)
            if month == 12:
                end_excl = f"{year + 1}-01-01"
            else:
                end_excl = f"{year}-{month + 1:02d}-01"
            periods.append({
                'start'   : f"{year}-{month:02d}-16",
                'end_excl': end_excl,
                'label'   : f"{year}-{month:02d}_2",
            })
    return periods


def get_monthly_periods(years):
    """Return list of dicts for each calendar month in the given years."""
    periods = []
    for year in years:
        for month in range(1, 13):
            start = datetime.date(year, month, 1)
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
# SECTION 1 — US Drought Monitor (USDM)
# =============================================================================
if DOWNLOAD_USDM:
    log("=" * 60)
    log("SECTION 1: US Drought Monitor (USDM)")
    log("=" * 60)

    periods  = get_bimonthly_periods(YEARS)
    existing = {f.stem for f in USDM_DIR.glob('USDM_CONUS_*.tif')}

    log(f"Output directory:    {USDM_DIR}")
    log(f"Total periods:       {len(periods)} (24 half-months x {len(YEARS)} years)")
    log(f"Already downloaded:  {len(existing)} — will skip these")
    log(f"Remaining to fetch:  {len(periods) - len(existing)}")
    log("")

    usdm_collection = ee.ImageCollection(USDM_COLLECTION)
    usdm_failed     = []

    for i, period in enumerate(periods):
        filename = f"USDM_CONUS_{period['label']}"
        out_path = USDM_DIR / f"{filename}.tif"

        if filename in existing:
            log(f"[{i+1:3d}/{len(periods)}] Skipped (exists): {filename}")
            continue

        try:
            # Filter to the bimonthly window and take the mean of all weekly
            # USDM snapshots that fall within it. For most half-months this
            # averages 1–2 images; the mean preserves fractional categories
            # which are useful for continuous analysis.
            image = (usdm_collection
                     .filterDate(period['start'], period['end_excl'])
                     .filterBounds(conus)
                     .select(USDM_BAND)
                     .mean())

            raw_bytes = compute_pixels_conus(image)

            with open(out_path, 'wb') as f:
                f.write(raw_bytes)

            log(f"[{i+1:3d}/{len(periods)}] Saved: {out_path.name}")

        except Exception as e:
            log(f"[{i+1:3d}/{len(periods)}] FAILED: {filename} — {e}")
            usdm_failed.append(period['label'])
            time.sleep(2)   # brief pause to avoid rate limits

    log("")
    log("USDM DOWNLOAD SUMMARY")
    log("-" * 40)
    log(f"Total periods:    {len(periods)}")
    log(f"Downloaded:       {len(periods) - len(existing) - len(usdm_failed)}")
    log(f"Skipped (exists): {len(existing)}")
    log(f"Failed:           {len(usdm_failed)}")

    if usdm_failed:
        log("")
        log("USDM failed periods (re-run to retry):")
        for label in usdm_failed:
            log(f"  {label}")
    log("")

else:
    log("SECTION 1 (USDM) skipped — DOWNLOAD_USDM=False")
    log("")


# =============================================================================
# SECTION 2 — GRIDMET Drought Metrics
# =============================================================================
if DOWNLOAD_GRIDMET:
    log("=" * 60)
    log("SECTION 2: GRIDMET Drought Metrics")
    log("=" * 60)

    periods  = get_monthly_periods(YEARS)
    existing = {f.stem for f in GRIDMET_DIR.glob('GRIDMET_drought_*.tif')}

    log(f"Output directory:    {GRIDMET_DIR}")
    log(f"Total periods:       {len(periods)} (12 months x {len(YEARS)} years)")
    log(f"Already downloaded:  {len(existing)} — will skip these")
    log(f"Remaining to fetch:  {len(periods) - len(existing)}")
    log(f"Bands per file:      {GRIDMET_BANDS}")
    log("")

    gridmet_collection = ee.ImageCollection(GRIDMET_COLLECTION)
    gridmet_failed     = []

    for i, period in enumerate(periods):
        filename = f"GRIDMET_drought_{period['label']}"
        out_path = GRIDMET_DIR / f"{filename}.tif"

        if filename in existing:
            log(f"[{i+1:3d}/{len(periods)}] Skipped (exists): {filename}")
            continue

        try:
            # GRIDMET/DROUGHT has 5-day composites. Take the mean of all images
            # within the calendar month to produce a monthly summary.
            # All 8 drought bands are selected and written as a single multi-
            # band GeoTIFF (band order matches GRIDMET_BANDS list above).
            image = (gridmet_collection
                     .filterDate(period['start'], period['end_excl'])
                     .filterBounds(conus)
                     .select(GRIDMET_BANDS)
                     .mean())

            raw_bytes = compute_pixels_conus(image)

            with open(out_path, 'wb') as f:
                f.write(raw_bytes)

            log(f"[{i+1:3d}/{len(periods)}] Saved: {out_path.name}")

        except Exception as e:
            log(f"[{i+1:3d}/{len(periods)}] FAILED: {filename} — {e}")
            gridmet_failed.append(period['label'])
            time.sleep(2)   # brief pause to avoid rate limits

    log("")
    log("GRIDMET DOWNLOAD SUMMARY")
    log("-" * 40)
    log(f"Total periods:    {len(periods)}")
    log(f"Downloaded:       {len(periods) - len(existing) - len(gridmet_failed)}")
    log(f"Skipped (exists): {len(existing)}")
    log(f"Failed:           {len(gridmet_failed)}")

    if gridmet_failed:
        log("")
        log("GRIDMET failed periods (re-run to retry):")
        for label in gridmet_failed:
            log(f"  {label}")
    log("")

else:
    log("SECTION 2 (GRIDMET) skipped — DOWNLOAD_GRIDMET=False")
    log("")

# =============================================================================
# Final summary
# =============================================================================
log("=" * 60)
log("ALL DROUGHT DOWNLOADS COMPLETE")
log("=" * 60)
log(f"USDM output:    {USDM_DIR}")
log(f"GRIDMET output: {GRIDMET_DIR}")
log(f"Log:            {log_file}")
