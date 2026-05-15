#!/usr/bin/env python3
"""
OpenET Monthly ET Download for Nebraska (2015-2024)
Runs as batch job on GRIT HPC

Mirror of 04_download_openet_iowa.py for Nebraska. Downloads OpenET ENSEMBLE
monthly ET from Google Earth Engine and saves GeoTIFFs aligned to the NLDAS
0.125° Nebraska reference grid.

Nebraska NLDAS reference grid (hardcoded — no GEE asset required):
  CRS:       EPSG:4326
  Transform: [0.125, 0, -104.125, 0, -0.125, 43.125]
  Dimensions: 71 cols × 25 rows
  Lon centers: -104.0625° to -95.3125° (0.125° step)
  Lat centers: 43.0625°  to 40.0625°  (0.125° step, north→south)

This grid is aligned to the NLDAS 0.125° CONUS grid, ensuring pixel-exact
correspondence with NLDAS Noah ET files used in the HumanET calculation.

Output: data/raw/openet/OpenET_Nebraska_YYYYMM.tif
"""

import sys
import subprocess
import io
import datetime
import time
from pathlib import Path

target_dir = str(Path.home() / '.local/lib/python3.12/site-packages')
if target_dir not in sys.path:
    sys.path.insert(0, target_dir)

try:
    import ee
    print(f'earthengine-api version: {ee.__version__}', flush=True)
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

OPENET_MONTHLY = 'projects/openet/assets/ensemble/conus/gridmet/monthly/v2_0'
ET_BAND        = 'et_ensemble_mad'

GEE_PROJECT = 'et-research-489120'

# ── Nebraska NLDAS 0.125° reference grid (NLDAS-aligned, no GEE asset needed) ─
# These values are derived from the NLDAS 0.125° CONUS grid specification.
# Cell centers: lon = -124.9375 + col*0.125, lat = 25.0625 + row*0.125
# Nebraska columns 167–237 (lon -104.0625 to -95.3125), rows 120–144 (lat 40.0625 to 43.0625)
NE_CRS       = 'EPSG:4326'
NE_TRANSFORM = [0.125, 0, -104.125, 0, -0.125, 43.125]  # [xScale, xShear, xOrig, yShear, yScale, yOrig]
NE_WIDTH     = 71    # columns: -104.0625 to -95.3125
NE_HEIGHT    = 25    # rows:     43.0625  to  40.0625

OUTPUT_DIR = Path('/home/pielab-sandbox-jcoldiron/SIF-Analysis/data/raw/openet')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

log_file = OUTPUT_DIR / f"download_log_nebraska_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"


def log(message):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f'[{timestamp}] {message}'
    print(line, flush=True)
    with open(log_file, 'a') as f:
        f.write(line + '\n')


# =============================================================================
# GEE init
# =============================================================================
log('Initializing Google Earth Engine...')
try:
    ee.Initialize(project=GEE_PROJECT)
    log('GEE initialized.')
except Exception as e:
    log(f'ERROR: {e}')
    import sys; sys.exit(1)

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
existing = {f.stem for f in OUTPUT_DIR.glob('OpenET_Nebraska_*.tif')}

log(f'Output: {OUTPUT_DIR}')
log(f'Periods: {len(periods)} | Already downloaded: {len(existing)} | Remaining: {len(periods) - len(existing)}')

collection = ee.ImageCollection(OPENET_MONTHLY)
failed     = []

for i, period in enumerate(periods):
    filename = f"OpenET_Nebraska_{period['label']}"
    out_path = OUTPUT_DIR / f'{filename}.tif'

    if filename in existing:
        log(f"[{i+1:3d}/{len(periods)}] Skipped (exists): {filename}")
        continue

    try:
        image = (collection
                 .filterDate(period['start'], period['end_excl'])
                 .filterBounds(nebraska)
                 .select(ET_BAND)
                 .mean())

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
                'dimensions': {
                    'width' : NE_WIDTH,
                    'height': NE_HEIGHT,
                },
            },
        })

        # Spatial coverage validation
        try:
            import numpy as _np
            import rasterio as _rio
            with _rio.open(io.BytesIO(raw_bytes)) as _src:
                _data = _src.read(1).astype(float)
                if _src.nodata is not None:
                    _data[_data == _src.nodata] = _np.nan
            _col_std  = _np.nanstd(_data, axis=0)
            _pct_vary = (_col_std > 0.01).mean()
            if _pct_vary < 0.8:
                raise ValueError(f'Coverage check FAILED ({_pct_vary:.0%} varying cols) — tile-only export?')
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
if failed:
    log('Failed periods: ' + ', '.join(failed))
