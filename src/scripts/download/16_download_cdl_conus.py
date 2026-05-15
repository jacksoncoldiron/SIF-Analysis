#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
USDA NASS CDL Download for Full CONUS via Google Earth Engine (2015-2024)
Downloads directly to data/raw/cdl/ on this server — no Google Drive needed.

PURPOSE:
--------
Export annual CDL crop-fraction rasters at the NLDAS 0.125° grid for the full
contiguous United States. Each output pixel contains a float fraction (0–1)
representing the share of ~1km CDL sub-pixels that fall into each crop class.

APPROACH:
---------
Download CDL as raw integer category codes at ~1km resolution (nearest-neighbor
sampling from native 30m) using computePixels — no reduceResolution needed,
so GEE only accesses one native pixel per output pixel (~20M total for CONUS,
well under the 2^31 limit). The 1km grid is defined as 14 sub-pixels per NLDAS
cell per dimension. Fractions are then computed client-side by counting
category pixels within each 14×14 NLDAS block using numpy.

WHY ~1km AND NOT 30m:
---------------------
30m CDL across full CONUS is ~22 billion pixels. Any GEE operation that needs
to aggregate those pixels (reduceResolution) hits a hard 2^31-pixel limit,
even in batch exports. Working at ~1km avoids this entirely. The resulting
fractions are computed from ~1km nearest-neighbor-sampled CDL rather than the
exact 30m pixels, which is adequate for NLDAS-resolution analysis.

NLDAS GRID:
-----------
  CRS:       EPSG:4326
  Transform: [0.125, 0, -125.0, 0, -0.125, 53.0]
  Size:      464 cols × 224 rows

CDL DOWNLOAD GRID (14 sub-pixels per NLDAS cell):
  Resolution: 0.125° / 14 ≈ 0.00893° ≈ 994m
  Size:       6496 cols × 3136 rows  (~20M pixels, ~20MB, fits in computePixels)

OUTPUT:
-------
  data/raw/cdl/CDL_CONUS_{year}.tif
  Bands: cropland_frac, corn_frac, soy_frac, wheat_frac, cotton_frac, pasture_frac
  Type:  float32, values 0–1
"""

import io
from pathlib import Path

import ee
import numpy as np
import rasterio
from rasterio.transform import Affine

# =============================================================================
# NLDAS grid
# =============================================================================
CONUS_CRS       = 'EPSG:4326'
CONUS_TRANSFORM = [0.125, 0, -125.0, 0, -0.125, 53.0]
CONUS_WIDTH     = 464
CONUS_HEIGHT    = 224

# =============================================================================
# CDL download grid (14 sub-pixels per NLDAS cell per dimension)
# =============================================================================
CDL_SUB       = 14                      # sub-pixels per NLDAS cell per axis
CDL_RES       = 0.125 / CDL_SUB        # 0.008928...°
CDL_WIDTH     = CONUS_WIDTH  * CDL_SUB  # 6496
CDL_HEIGHT    = CONUS_HEIGHT * CDL_SUB  # 3136
CDL_TRANSFORM = [CDL_RES, 0, -125.0, 0, -CDL_RES, 53.0]

# =============================================================================
# GEE / CDL configuration
# =============================================================================
COLLECTION  = 'USDA/NASS/CDL'
CDL_BAND    = 'cropland'
GEE_PROJECT = 'ee-jacksoncoldiron'
YEARS       = list(range(2015, 2025))

BAND_NAMES = ['cropland_frac', 'corn_frac', 'soy_frac',
              'wheat_frac',    'cotton_frac', 'pasture_frac']

# Vectorised category tests (applied to a numpy uint8 array)
BAND_TESTS = [
    lambda c: c <= 99,                          # cropland (codes 1–99)
    lambda c: c == 1,                           # corn
    lambda c: c == 5,                           # soy
    lambda c: (c == 22) | (c == 23) | (c == 24),  # wheat (winter/spring/durum)
    lambda c: c == 2,                           # cotton
    lambda c: (c == 36) | (c == 37),            # pasture (alfalfa / other hay)
]

OUT_DIR = Path(__file__).parents[3] / 'data' / 'raw' / 'cdl'
OUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# GEE authentication
# =============================================================================
ee.Initialize(project=GEE_PROJECT)

# =============================================================================
# Download one year
# =============================================================================

def download_year(year):
    out_path = OUT_DIR / f'CDL_CONUS_{year}.tif'
    if out_path.exists():
        print(f'  {year}: already exists, skipping.')
        return

    # --- fetch CDL at ~1km via nearest-neighbor (no reduceResolution) ---
    # Requesting at CDL_RES ≈ 994m: GEE picks the nearest native 30m pixel
    # per output cell — only ~20M pixel lookups total, no 2^31 issue.
    print(f'  {year}: downloading CDL at ~1km...', end=' ', flush=True)
    cdl_img = (ee.ImageCollection(COLLECTION)
               .filterDate(f'{year}-01-01', f'{year}-12-31')
               .first()
               .select(CDL_BAND))

    raw = ee.data.computePixels({
        'expression': cdl_img,
        'fileFormat': 'GEO_TIFF',
        'grid': {
            'dimensions': {'width': CDL_WIDTH, 'height': CDL_HEIGHT},
            'affineTransform': {
                'scaleX':     CDL_TRANSFORM[0],
                'shearX':     CDL_TRANSFORM[1],
                'translateX': CDL_TRANSFORM[2],
                'shearY':     CDL_TRANSFORM[3],
                'scaleY':     CDL_TRANSFORM[4],
                'translateY': CDL_TRANSFORM[5],
            },
            'crsCode': CONUS_CRS,
        },
    })

    with rasterio.open(io.BytesIO(raw)) as src:
        cdl = src.read(1)   # shape: (CDL_HEIGHT, CDL_WIDTH), dtype uint8

    dl_mb = len(raw) / 1e6
    print(f'got {dl_mb:.1f} MB. Computing fractions...', end=' ', flush=True)

    # --- client-side fraction computation ---
    # Reshape into (CONUS_HEIGHT, CDL_SUB, CONUS_WIDTH, CDL_SUB) so that
    # cdl_blocks[i, :, j, :] is the 14×14 block of CDL pixels for NLDAS cell (i, j)
    cdl_blocks = cdl.reshape(CONUS_HEIGHT, CDL_SUB, CONUS_WIDTH, CDL_SUB)
    n_sub = CDL_SUB * CDL_SUB  # 196 pixels per NLDAS cell

    bands = []
    for test in BAND_TESTS:
        mask = test(cdl_blocks)                        # bool (224, 14, 464, 14)
        frac = mask.sum(axis=(1, 3)) / n_sub           # float64 (224, 464)
        bands.append(frac.astype(np.float32))

    # --- write NLDAS-resolution GeoTIFF ---
    affine = Affine(CONUS_TRANSFORM[0], CONUS_TRANSFORM[1], CONUS_TRANSFORM[2],
                    CONUS_TRANSFORM[3], CONUS_TRANSFORM[4], CONUS_TRANSFORM[5])
    with rasterio.open(
        out_path, 'w',
        driver='GTiff', compress='lzw',
        height=CONUS_HEIGHT, width=CONUS_WIDTH,
        count=len(BAND_NAMES), dtype='float32',
        crs='EPSG:4326', transform=affine,
    ) as dst:
        dst.write(np.stack(bands))
        for i, name in enumerate(BAND_NAMES, 1):
            dst.set_band_description(i, name)

    size_mb = out_path.stat().st_size / 1e6
    print(f'saved ({size_mb:.1f} MB) → {out_path}')


# =============================================================================
# Main
# =============================================================================

print(f'Downloading CDL CONUS crop-fraction GeoTIFFs for years: {YEARS}')
print(f'GEE project : {GEE_PROJECT}')
print(f'Output dir  : {OUT_DIR}')
print(f'CDL grid    : {CDL_WIDTH}×{CDL_HEIGHT} pixels at {CDL_RES:.5f}° (~994m)')
print(f'NLDAS grid  : {CONUS_WIDTH}×{CONUS_HEIGHT} pixels at 0.125° per {CDL_SUB}×{CDL_SUB} CDL block')
print()

for year in YEARS:
    download_year(year)

print()
print('All done.')
print()
print('Each file has 6 float32 bands:')
print('  1. cropland_frac  — any code <= 99 (all agricultural land)')
print('  2. corn_frac      — code 1')
print('  3. soy_frac       — code 5')
print('  4. wheat_frac     — codes 22, 23, 24')
print('  5. cotton_frac    — code 2')
print('  6. pasture_frac   — codes 36, 37 (alfalfa / other hay)')
