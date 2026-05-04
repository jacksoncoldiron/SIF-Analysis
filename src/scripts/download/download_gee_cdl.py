#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
USDA NASS Cropland Data Layer (CDL) Download via Google Earth Engine
Iowa, 2015-2024 (annual, January composite exported as GeoTIFF to Google Drive)

PURPOSE:
--------
Download annual CDL cropland classification maps for Iowa to serve as the
agricultural pixel mask and crop-type classifier for the 10-year SIF irrigation
analysis (2015-2024).

CURRENT STATUS:
---------------
Years 2019-2024 are already on disk at:
  data/raw/Cornbelt_annual_CDL/Cornbelt_annual_CDL_cropland_YYYY-01.tif

Years 2015-2018 still need to be downloaded. This script submits GEE batch
export tasks for 2015-2018 only. After running, check task status at:
  https://console.cloud.google.com/earth-engine/tasks?project=ee-jacksoncoldiron
Then download from Google Drive and place in the same directory as 2019-2024.

CDL CROP CODES (key values used in analysis):
  1   = Corn
  5   = Soybeans
  <100 = Crops (all agricultural land)
  ≥100 = Non-agricultural (water, developed, forest, etc.)

OUTPUT:
-------
Google Drive → download manually to:
  data/raw/Cornbelt_annual_CDL/Cornbelt_annual_CDL_cropland_YYYY-01.tif
"""

import ee

# ─── GEE authentication ───────────────────────────────────────────────────────
# Uses cached credentials at ~/.config/earthengine/credentials
ee.Initialize(project='ee-jacksoncoldiron')

# ─── Region of interest ───────────────────────────────────────────────────────
# Iowa state boundary from the US Census TIGER dataset
roi = (ee.FeatureCollection("TIGER/2018/States")
         .filter(ee.Filter.eq('NAME', 'Iowa'))
         .geometry())

# ─── Years to download ────────────────────────────────────────────────────────
# Only submit tasks for years NOT already on disk.
# 2019-2024 are already downloaded; submit 2015-2018 only.
# Change YEARS_TO_DOWNLOAD if you need to re-download any year.
YEARS_TO_DOWNLOAD = [2015, 2016, 2017, 2018]

# ─── CDL collection settings ──────────────────────────────────────────────────
COLLECTION   = 'USDA/NASS/CDL'
VARIABLE     = 'cropland'  # annual crop type classification (0-254)
EXPORT_NAME  = 'Cornbelt_annual_CDL'
SCALE        = 500          # 500m resolution (native ~30m; aggregated to save file size)
CRS          = 'EPSG:4326'

# ─── Load and filter the CDL ImageCollection ─────────────────────────────────
dataset = (ee.ImageCollection(COLLECTION)
             .filterDate(f'{YEARS_TO_DOWNLOAD[0]}-01-01',
                         f'{YEARS_TO_DOWNLOAD[-1]}-12-31')
             .select(VARIABLE))

# Attach a formatted date string to each image for easy filtering
def add_date_property(image):
    date = ee.Date(image.get('system:time_start'))
    return image.set('date', date.format('YYYY-MM'))

collection_with_date = dataset.map(add_date_property)

# CDL is annual so each year has exactly one image (labeled YYYY-01)
dates = collection_with_date.aggregate_array('date').distinct().getInfo()
print(f"Found CDL images for dates: {dates}")

# ─── Export function ──────────────────────────────────────────────────────────
def export_cdl_for_date(date_str):
    """Submit a GEE batch export task for one CDL annual image."""
    # Filter to this specific date, clip to Iowa
    img = (collection_with_date
               .filter(ee.Filter.eq('date', date_str))
               .select(VARIABLE)
               .mean()               # mean of 1 image = itself
               .clip(roi)
               .toDouble())

    # Export to Google Drive
    task = ee.batch.Export.image.toDrive(
        image       = img,
        description = f"{EXPORT_NAME}_{VARIABLE}_{date_str}",
        region      = roi,
        maxPixels   = 1e13,
        scale       = SCALE,
        crs         = CRS,
        fileFormat  = 'GeoTIFF',
    )
    task.start()
    print(f"  Submitted: {EXPORT_NAME}_{VARIABLE}_{date_str}")

# ─── Submit export tasks ──────────────────────────────────────────────────────
# Only submit tasks for target years (skip dates already on disk)
target_year_strs = [f'{y}-01' for y in YEARS_TO_DOWNLOAD]

print(f"\nSubmitting CDL export tasks for years: {YEARS_TO_DOWNLOAD}")
submitted = 0
for date_str in sorted(dates):
    if date_str in target_year_strs:
        export_cdl_for_date(date_str)
        submitted += 1

print(f"\n{submitted} task(s) submitted.")
print("\nNext steps:")
print("  1. Monitor export tasks at:")
print("     https://console.cloud.google.com/earth-engine/tasks?project=ee-jacksoncoldiron")
print("  2. Once complete, download from Google Drive to:")
print("     data/raw/Cornbelt_annual_CDL/Cornbelt_annual_CDL_cropland_YYYY-01.tif")
print("  3. Verify all 10 years (2015-2024) are present before running human_et.ipynb")
