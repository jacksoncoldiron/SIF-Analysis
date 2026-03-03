#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script to pull drought data at county level for Iowa from GEE
Modified from cropland CDL script by Sophie Ruehr

@author: sophieruehr
@modified: jacksoncoldiron
"""
import ee
import datetime
import time

# Initialize Earth Engine
PROJECT_NAME = 'ee-jacksoncoldiron'
ee.Initialize(project=PROJECT_NAME)

# Input parameters
roi = ee.FeatureCollection("TIGER/2018/Counties") \
    .filter(ee.Filter.eq('STATEFP', '19')) \
    .geometry()

start_date = '2015-01-01'
end_date = '2025-01-01'
variable = 'DM'  # Drought Monitor classification band
collectionname = 'Iowa_county_drought'
filename = collectionname + '_' + variable

# Load dataset
collection = 'projects/sat-io/open-datasets/us-drought-monitor'
dataset = ee.ImageCollection(collection).filterDate(start_date, end_date).select(variable)

# Generate half-monthly periods
half_monthly_periods = []
start = datetime.datetime(2015, 1, 1)
end = datetime.datetime(2025, 1, 1)
current = start

while current < end:
    # First half: 1st-15th
    first_half_start = current.replace(day=1)
    first_half_end = current.replace(day=15)
    half_monthly_periods.append({
        'start': first_half_start,
        'end': first_half_end,
        'label': f"{first_half_start.strftime('%Y-%m')}_1"
    })
    
    # Second half: 16th-end of month
    if current.month == 12:
        second_half_end = datetime.datetime(current.year + 1, 1, 1)
    else:
        second_half_end = datetime.datetime(current.year, current.month + 1, 1)
    
    second_half_start = current.replace(day=16)
    half_monthly_periods.append({
        'start': second_half_start,
        'end': second_half_end,
        'label': f"{second_half_start.strftime('%Y-%m')}_2"
    })
    
    # Move to next month
    if current.month == 12:
        current = datetime.datetime(current.year + 1, 1, 1)
    else:
        current = datetime.datetime(current.year, current.month + 1, 1)

# Export function
def export_images_for_period(period):
    start_str = period['start'].strftime('%Y-%m-%d')
    end_str = period['end'].strftime('%Y-%m-%d')
    label = period['label']
    
    collection_filtered = dataset \
        .filterDate(start_str, end_str) \
        .select(variable) \
        .mean() \
        .clip(roi) \
        .toDouble()
    
    task = ee.batch.Export.image.toDrive(
        image=collection_filtered,
        description=f"{filename}_{label}",
        region=roi,
        maxPixels=1e13,
        scale=1000,
        crs='EPSG:4326',
        fileFormat='GeoTIFF'
    )
    
    task.start()

# Submit export tasks
for i, period in enumerate(half_monthly_periods, 1):
    print(f"Submitting task {i}/{len(half_monthly_periods)}: {period['label']}")
    export_images_for_period(period)
    if i < len(half_monthly_periods):
        time.sleep(0.5)

print(f"\nAll {len(half_monthly_periods)} tasks submitted.")

# Check https://console.cloud.google.com/earth-engine/tasks?project=ee-jacksoncoldiron
# to track your status
