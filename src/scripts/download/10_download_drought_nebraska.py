#!/usr/bin/env python3
"""
US Drought Monitor — County-level Download for Nebraska (2015-2025)
Mirror of 09_download_drought_iowa.py for Nebraska (FIPS 31).

Downloads half-monthly USDM drought rasters at county level for Nebraska counties,
saved as GeoTIFFs to Google Drive, then download manually.

Nebraska FIPS: 31  (Iowa is 19)

OUTPUT:
-------
Google Drive → download manually to:
  data/raw/drought_usdm/
  (files named: USDM_Nebraska_DM_YYYY-MM_1.tif  and  ..._2.tif)
"""

import ee
import datetime
import time

ee.Initialize(project='ee-jacksoncoldiron')

roi = (ee.FeatureCollection('TIGER/2018/Counties')
         .filter(ee.Filter.eq('STATEFP', '31'))   # 31 = Nebraska
         .geometry())

start_date = '2015-01-01'
end_date   = '2025-01-01'
variable   = 'DM'
collection_name = 'Nebraska_county_drought'
filename    = collection_name + '_' + variable

collection = 'projects/sat-io/open-datasets/us-drought-monitor'
dataset = ee.ImageCollection(collection).filterDate(start_date, end_date).select(variable)

# ─── Generate half-monthly periods ───────────────────────────────────────────
half_monthly_periods = []
start   = datetime.datetime(2015, 1, 1)
end     = datetime.datetime(2025, 1, 1)
current = start

while current < end:
    first_half_end = current.replace(day=15)
    half_monthly_periods.append({
        'start': current.replace(day=1),
        'end'  : first_half_end,
        'label': f"{current.strftime('%Y-%m')}_1",
    })
    if current.month == 12:
        second_half_end = datetime.datetime(current.year + 1, 1, 1)
    else:
        second_half_end = datetime.datetime(current.year, current.month + 1, 1)

    half_monthly_periods.append({
        'start': current.replace(day=16),
        'end'  : second_half_end,
        'label': f"{current.strftime('%Y-%m')}_2",
    })

    current = (datetime.datetime(current.year + 1, 1, 1)
               if current.month == 12
               else datetime.datetime(current.year, current.month + 1, 1))

# ─── Export function ──────────────────────────────────────────────────────────
def export_for_period(period):
    start_str = period['start'].strftime('%Y-%m-%d')
    end_str   = period['end'].strftime('%Y-%m-%d')
    label     = period['label']

    img = (dataset
           .filterDate(start_str, end_str)
           .select(variable)
           .mean()
           .clip(roi)
           .toDouble())

    task = ee.batch.Export.image.toDrive(
        image       = img,
        description = f'{filename}_{label}',
        region      = roi,
        maxPixels   = 1e13,
        scale       = 1000,
        crs         = 'EPSG:4326',
        fileFormat  = 'GeoTIFF',
    )
    task.start()

# ─── Submit tasks ─────────────────────────────────────────────────────────────
for i, period in enumerate(half_monthly_periods, 1):
    print(f'Submitting {i}/{len(half_monthly_periods)}: {period["label"]}')
    export_for_period(period)
    if i < len(half_monthly_periods):
        time.sleep(0.5)

print(f'\n{len(half_monthly_periods)} tasks submitted.')
print('Monitor at: https://console.cloud.google.com/earth-engine/tasks?project=ee-jacksoncoldiron')
print('Download from Google Drive to: data/raw/drought_usdm/')
