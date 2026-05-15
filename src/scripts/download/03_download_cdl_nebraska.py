#!/usr/bin/env python3
"""
USDA NASS Cropland Data Layer (CDL) Download for Nebraska via Google Earth Engine
2015-2024 (annual, exported as GeoTIFF to Google Drive)

Mirror of 02_download_cdl_iowa.py for Nebraska state boundary.

CDL CROP CODES (key values used in analysis):
  1   = Corn
  5   = Soybeans
  <100 = Crops (all agricultural land)
  ≥100 = Non-agricultural

Nebraska FIPS: 31  |  State bbox: ~104.05°W–95.31°W, 40.00°N–43.00°N

OUTPUT:
-------
Google Drive → download manually to:
  data/raw/cdl/CDL_Nebraska_YYYY-01.tif
"""

import ee

ee.Initialize(project='ee-jacksoncoldiron')

# ─── Region of interest ───────────────────────────────────────────────────────
roi = (ee.FeatureCollection('TIGER/2018/States')
         .filter(ee.Filter.eq('NAME', 'Nebraska'))
         .geometry())

# ─── Years to download ────────────────────────────────────────────────────────
YEARS_TO_DOWNLOAD = list(range(2015, 2025))   # 2015–2024

# ─── CDL collection settings ──────────────────────────────────────────────────
COLLECTION   = 'USDA/NASS/CDL'
VARIABLE     = 'cropland'
EXPORT_NAME  = 'CDL_Nebraska'
SCALE        = 30     # native 30m resolution — preserves exact integer crop codes
CRS          = 'EPSG:4326'

# NOTE: Using native 30m scale (not 500m) to avoid bilinear resampling artifacts
# on categorical crop codes. The 500m Iowa CDL files have interpolated float values
# that break exact-code comparisons in load_crop_type_fractions(). At 30m, each
# pixel retains its exact CDL integer code.

# ─── Load and filter the CDL ImageCollection ─────────────────────────────────
dataset = (ee.ImageCollection(COLLECTION)
             .filterDate(f'{YEARS_TO_DOWNLOAD[0]}-01-01',
                         f'{YEARS_TO_DOWNLOAD[-1]}-12-31')
             .select(VARIABLE))

def add_date_property(image):
    date = ee.Date(image.get('system:time_start'))
    return image.set('date', date.format('YYYY-MM'))

collection_with_date = dataset.map(add_date_property)
dates = collection_with_date.aggregate_array('date').distinct().getInfo()
print(f'Found CDL images for dates: {sorted(dates)}')

# ─── Export function ──────────────────────────────────────────────────────────
def export_cdl_for_date(date_str):
    img = (collection_with_date
               .filter(ee.Filter.eq('date', date_str))
               .select(VARIABLE)
               .first()          # single annual image — no averaging needed
               .clip(roi)
               .toInt16())       # keep as integer to avoid interpolation artifacts

    task = ee.batch.Export.image.toDrive(
        image       = img,
        description = f'{EXPORT_NAME}_{VARIABLE}_{date_str}',
        region      = roi,
        maxPixels   = 1e13,
        scale       = SCALE,
        crs         = CRS,
        fileFormat  = 'GeoTIFF',
    )
    task.start()
    print(f'  Submitted: {EXPORT_NAME}_{VARIABLE}_{date_str}')

# ─── Submit export tasks ──────────────────────────────────────────────────────
target_year_strs = [f'{y}-01' for y in YEARS_TO_DOWNLOAD]

print(f'\nSubmitting CDL Nebraska export tasks for {YEARS_TO_DOWNLOAD[0]}–{YEARS_TO_DOWNLOAD[-1]}')
submitted = 0
for date_str in sorted(dates):
    if date_str in target_year_strs:
        export_cdl_for_date(date_str)
        submitted += 1

print(f'\n{submitted} task(s) submitted.')
print('\nNext steps:')
print('  1. Monitor at https://console.cloud.google.com/earth-engine/tasks?project=ee-jacksoncoldiron')
print('  2. Download from Google Drive to:')
print('     data/raw/cdl/CDL_Nebraska_YYYY-01.tif')
