#!/usr/bin/env python3
"""
MODIS Monthly NDVI Download for Nebraska (2015-2024)
Runs as batch job on GRIT HPC

Mirror of 07_download_ndvi_iowa.py for Nebraska. Downloads MODIS Terra MOD13A3
monthly NDVI from Google Earth Engine and saves GeoTIFFs aligned to the NLDAS
0.125° Nebraska reference grid.

PRODUCT:
--------
MOD13A3.061 — MODIS Terra Vegetation Indices (1km, Monthly)
GEE Asset: MODIS/061/MOD13A3
Band: 'NDVI' (integer * 10000; divide by 0.0001 to get true NDVI)

Nebraska NLDAS reference grid:
  Transform: [0.125, 0, -104.125, 0, -0.125, 43.125]
  Dimensions: 71 cols × 25 rows

Output: data/raw/ndvi/NDVI_Nebraska_YYYYMM.tif
"""

import sys
import subprocess
import datetime
import time
from pathlib import Path

target_dir = str(Path.home() / '.local/lib/python3.12/site-packages')
if target_dir not in sys.path:
    sys.path.insert(0, target_dir)

try:
    import ee
except ImportError:
    subprocess.check_call([
        sys.executable, '-m', 'pip', 'install',
        '--target=' + target_dir, 'earthengine-api'
    ])
    import ee

# =============================================================================
# Configuration
# =============================================================================

YEARS = list(range(2015, 2025))

MODIS_MONTHLY = 'MODIS/061/MOD13A3'
NDVI_BAND     = 'NDVI'
NDVI_SCALE    = 0.0001   # integer * 10000 → true NDVI

GEE_PROJECT = 'et-research-489120'

# Nebraska NLDAS 0.125° reference grid
NE_CRS       = 'EPSG:4326'
NE_TRANSFORM = [0.125, 0, -104.125, 0, -0.125, 43.125]
NE_WIDTH     = 71
NE_HEIGHT    = 25

OUTPUT_DIR = Path('/home/pielab-sandbox-jcoldiron/SIF-Analysis/data/raw/ndvi')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

log_file = OUTPUT_DIR / f"download_log_nebraska_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"


def log(message):
    ts   = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f'[{ts}] {message}'
    print(line, flush=True)
    with open(log_file, 'a') as f:
        f.write(line + '\n')


log('Initializing GEE...')
ee.Initialize(project=GEE_PROJECT)
log('GEE initialized.')

nebraska = (ee.FeatureCollection('TIGER/2018/States')
              .filter(ee.Filter.eq('NAME', 'Nebraska'))
              .geometry())

# =============================================================================
# Monthly period generator
# =============================================================================
def get_monthly_periods(years):
    periods = []
    for year in years:
        for month in range(1, 13):
            start = datetime.date(year, month, 1)
            end_excl = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)
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
existing = {f.stem for f in OUTPUT_DIR.glob('NDVI_Nebraska_*.tif')}

log(f'Periods: {len(periods)} | Already downloaded: {len(existing)} | Remaining: {len(periods) - len(existing)}')

collection = ee.ImageCollection(MODIS_MONTHLY)
failed     = []

for i, period in enumerate(periods):
    filename = f"NDVI_Nebraska_{period['label']}"
    out_path = OUTPUT_DIR / f'{filename}.tif'

    if filename in existing:
        log(f"[{i+1:3d}/{len(periods)}] Skipped: {filename}")
        continue

    try:
        image = (collection
                 .filterDate(period['start'], period['end_excl'])
                 .filterBounds(nebraska)
                 .select(NDVI_BAND)
                 .mean()
                 .multiply(NDVI_SCALE))

        raw_bytes = ee.data.computePixels({
            'expression': image,
            'fileFormat': 'GEO_TIFF',
            'grid': {
                'crsCode': NE_CRS,
                'affineTransform': {
                    'scaleX'    : NE_TRANSFORM[0],
                    'shearX'    : NE_TRANSFORM[1],
                    'translateX': NE_TRANSFORM[2],
                    'shearY'    : NE_TRANSFORM[3],
                    'scaleY'    : NE_TRANSFORM[4],
                    'translateY': NE_TRANSFORM[5],
                },
                'dimensions': {'width': NE_WIDTH, 'height': NE_HEIGHT},
            },
        })

        # Range validation
        try:
            import io as _io
            import numpy as _np
            import rasterio as _rio
            with _rio.open(_io.BytesIO(raw_bytes)) as _src:
                _data = _src.read(1).astype(float)
                if _src.nodata is not None:
                    _data[_data == _src.nodata] = _np.nan
            _valid = _data[~_np.isnan(_data)]
            if len(_valid) > 0:
                vmin, vmax = float(_np.nanmin(_valid)), float(_np.nanmax(_valid))
                if vmin < -1.5 or vmax > 1.5:
                    log(f'  WARNING: NDVI range [{vmin:.3f}, {vmax:.3f}] — check scale factor')
        except ImportError:
            pass

        with open(out_path, 'wb') as f:
            f.write(raw_bytes)

        log(f"[{i+1:3d}/{len(periods)}] Saved: {out_path.name}")

    except Exception as e:
        log(f"[{i+1:3d}/{len(periods)}] FAILED: {filename} — {e}")
        failed.append(period['label'])
        time.sleep(2)

log('')
log('=' * 60)
log(f'Downloaded: {len(periods) - len(existing) - len(failed)}'
    f'  |  Skipped: {len(existing)}'
    f'  |  Failed: {len(failed)}')
