#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 09:58:53 2024

@author: sophieruehr
"""
# Import the Google Earth Engine API
import ee

# Trigger the authentication flow. You'll need to have a user login and project already.
ee.Authenticate()

# Initialize the library (switch to your username)
ee.Initialize(project='ee-jacksoncoldiron') 

# ** Input desired information: area of interest, dates, months, resampling scale

# 1: Region of interest
    # Input the state or a lat/lon bounding box
roi = ee.FeatureCollection("TIGER/2018/States") \
    .filter(ee.Filter.eq('NAME', 'Iowa')) \
    .geometry()

# 2: Dates of interest - select your time periods
start_date = '2010-01-01'
end_date = '2025-01-01'

# 3: Months of interest - only relevant if you're doing seasonal stuff
first_month = 1
last_month = 1 # Change this to 12 if you're doing monthly stuff

# 4: Variable of interest - this selects the band from the dataset
    # For CDL, there are multiple bands, see here: https://developers.google.com/earth-engine/datasets/catalog/USDA_NASS_CDL#bands'
    # Can mask out uncultivated land with the 'cultivated' band
    # Can look at specific crop species with the 'cropland' band
variable = 'cropland' 

# 5: Set export name
collectionname = 'Cornbelt_annual_CDL' # You can add another string here to help ID the exports
filename = collectionname + '_' + variable
print(filename)

# 6: Set name of collection - you could sub in another GEE product here
collection = 'USDA/NASS/CDL'

# ** Open data and visualize at native resolution
dataset = ee.ImageCollection(collection) \
    .filterDate(start_date, end_date) \
    .select(variable) \

print(dataset)

# ** Export: loop through each image + reduce resolution
# Add date property to collection
def addDateProperty(image):
    date = ee.Date(image.get('system:time_start'))
    return image.set('date', date.format('YYYY-MM'))

collectionWithDate = dataset.map(addDateProperty)
dates = collectionWithDate.aggregate_array('date').distinct().getInfo()

# Function to export images
def export_images_for_date(date):
    # Filter the collection for the specific date
    collection_filtered = collectionWithDate.filter(ee.Filter.eq('date', date)).select(variable)
    
    # If relevant, you can take the mean value over this time period
    collection_filtered = collection_filtered.mean()
    
    # Clip the image to the desired region
    image_clipped = collection_filtered.clip(roi).toDouble()
    
        
    # Export the image to Google Drive
    task = ee.batch.Export.image.toDrive(
        image=image_clipped,
        description=f"{filename}_{date}",
        region=roi,
        maxPixels=1e13,
        scale=500, # Select the spatial scale
        crs='EPSG:4326',
        fileFormat='GeoTIFF'
    )
    
    task.start()


# Loop over each date and call the export function
for date in dates:
    export_images_for_date(date)

print("All tasks are submitted.")

# Check https://console.cloud.google.com/earth-engine/tasks?project=ee-jacksoncoldiron
# to track your status; you'll need to sub your ee-project name
