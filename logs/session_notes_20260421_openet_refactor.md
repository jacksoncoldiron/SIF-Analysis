# Session Notes — OpenET Download Script Refactor
**Date:** 2026-04-21  
**Project:** SIF-Analysis  
**File modified:** `src/scripts/download/openet_download.py`

---

## Problem

The previous version of `openet_download.py` used a hardcoded target grid:

```python
TARGET_CRS       = 'EPSG:4326'
TARGET_TRANSFORM = [0.05, 0, -180.0, 0, -0.05, 90.0]  # SIF OCO-2 0.05° grid
```

This was aligned to the SIF OCO-2 global 0.05° grid, but the project's NLDAS data is on a 0.125° grid. The OpenET and NLDAS layers couldn't be compared cell-by-cell without resampling. The goal was to align the OpenET downloads to the NLDAS grid instead, and do so in a way that doesn't require hardcoding grid parameters.

---

## Solution: GEE Reference Asset Approach

Instead of hardcoding the grid, we upload a reference GeoTIFF to GEE as an asset and have the script read the CRS, pixel size, and alignment from it at runtime using `ee.Image.projection().getInfo()`.

**Key GEE API calls used:**
```python
ref_image    = ee.Image('projects/.../assets/NLDAS_Iowa_reference_grid')
ref_proj     = ref_image.projection().getInfo()
TARGET_CRS       = ref_proj['crs']        # e.g. 'EPSG:4326'
TARGET_TRANSFORM = ref_proj['transform']  # e.g. [0.125, 0, -96.75, 0, -0.125, 43.5]
ref_region   = ref_image.geometry()       # bounding box extent for download region
```

This makes the grid definition authoritative in one place (the GEE asset) rather than scattered across scripts.

---

## Reference TIF: Creation and Upload

### Why the original uploaded file was wrong

The first TIF uploaded (`PET_12_December_2023_Iowa_mm_day`) was generated from a processed NLDAS file that was not flipped to north-up orientation. NLDAS NetCDF files store latitude in ascending order (south → north), which produces a GeoTIFF with a positive y-scale — meaning the image is stored south-up (upside down in GIS tools).

The processing notebooks (`nldas_et_download.ipynb`, `NLDAS_PET_download-checkpoint.ipynb`) have a flip correction:
```python
if 'lat' in mean_et.dims and mean_et.lat[0] < mean_et.lat[-1]:
    mean_et = mean_et.sortby('lat', ascending=False)
```
But this was only applied to the **mean** TIFs, not individual monthly outputs. All existing `ET_delta/` TIFs are also south-up for the same reason.

### Correct reference TIF generated from raw data

A properly north-up reference TIF was generated directly from `data/raw/NLDAS_Noah/NLDAS_NOAH0125_M.A202312.020.nc` using rasterio's `from_origin()`:

```python
from rasterio.transform import from_origin
# NW corner origin, positive pixel size → rasterio produces negative y-scale internally
transform = from_origin(x_origin, y_origin, res, res)  # res = 0.125
```

Output: `data/raw/NLDAS_Noah/NLDAS_Iowa_reference_grid.tif`

| Property | Value |
|---|---|
| CRS | EPSG:4326 |
| Resolution | 0.125° × 0.125° |
| Orientation | North-up (y-scale = -0.125) |
| Extent | -96.75 → -90.125 lon, 40.375 → 43.5 lat |
| Grid size | 53 cols × 25 rows |

This file was uploaded to GEE as:
```
projects/et-research-489120/assets/NLDAS_Iowa_reference_grid
```

---

## Script Changes

### 1. Replaced hardcoded grid with reference asset config

**Before:**
```python
TARGET_CRS       = 'EPSG:4326'
TARGET_TRANSFORM = [0.05, 0, -180.0, 0, -0.05, 90.0]
```

**After:**
```python
REF_ASSET_ID = 'projects/et-research-489120/assets/NLDAS_Iowa_reference_grid'
```

### 2. Added runtime projection read after GEE init

```python
ref_image        = ee.Image(REF_ASSET_ID)
ref_proj_info    = ref_image.projection().getInfo()
TARGET_CRS       = ref_proj_info['crs']
TARGET_TRANSFORM = ref_proj_info['transform']
ref_region       = ref_image.geometry()
```

At runtime this logs:
```
CRS:       EPSG:4326
Transform: [0.125, 0, -96.75, 0, -0.125, 43.5]
```

### 3. Iowa geometry now used only for collection filtering

The Iowa TIGER polygon is still used for `filterBounds()` to narrow the OpenET image collection, but is no longer used for `.clip()` or as the download region.

### 4. Removed `.clip(iowa)` — download now covers full bounding box

**Before:**
```python
image = (collection...mean().clip(iowa))
url = image.getDownloadURL({'region': iowa, ...})
```

**After:**
```python
image = (collection...mean())  # no clip
url = image.getDownloadURL({'region': ref_region, ...})
```

Using the rectangular bounding box as the download region ensures no edge pixels that intersect Iowa get clipped off. Clipping to the exact Iowa boundary shape happens in post-processing.

---

## Test Run (2026-04-21 09:14)

Script ran against existing 60-file dataset (2019–2023 already downloaded). All files were skipped as expected. GEE init and reference asset projection read completed successfully.

**Reference grid transform confirmed:** `[0.125, 0, -96.75, 0, -0.125, 43.5]` — negative y-scale confirms north-up orientation is correct.

---

## Additional Issues Found and Fixed During Re-download

### Problem 1: Deprecated OpenET collection ID
Updated `OPENET_MONTHLY`:
```python
# Before
OPENET_MONTHLY = 'OpenET/ENSEMBLE/CONUS/GRIDMET/MONTHLY/v2_0'
# After
OPENET_MONTHLY = 'projects/openet/assets/ensemble/conus/gridmet/monthly/v2_0'
```

### Problem 2: getDownloadURL + crsTransform silently fails for UTM→WGS84
First re-download produced 7×4 pixels at 1°/pixel. Root cause: OpenET's native CRS is **EPSG:32610 (UTM Zone 10N, 30m)**. When GEE reprojects from UTM to WGS84 via `getDownloadURL`, it silently falls back to 1 unit/pixel of the target CRS (1°) rather than the requested 0.125°. Neither `.reproject()` nor the `crsTransform` param in `getDownloadURL` reliably override this.

**Fix: replaced `getDownloadURL` with `ee.data.computePixels()`**, which accepts an explicit affine grid:
```python
raw_bytes = ee.data.computePixels({
    'expression': image,          # pass ee.Image directly — do NOT use ee.serializer.encode()
    'fileFormat': 'GEO_TIFF',
    'grid': {
        'crsCode': TARGET_CRS,
        'affineTransform': {
            'scaleX': TRANSFORM[0], 'shearX': TRANSFORM[1], 'translateX': TRANSFORM[2],
            'shearY': TRANSFORM[3], 'scaleY': TRANSFORM[4], 'translateY': TRANSFORM[5],
        },
        'dimensions': {'width': REF_WIDTH, 'height': REF_HEIGHT},
    },
})
```

Also removed the now-unnecessary `requests` and `zipfile` imports (`computePixels` returns raw bytes directly).

### Problem 3: ref_image.geometry() returns irregular polygon
`ref_image.geometry()` on a small uploaded TIF returns a slightly warped polygon, not a clean rectangle. Since `computePixels` uses explicit dimensions rather than a region, this is no longer used in the download loop. Reference grid dimensions are read from `ref_image.getInfo()['bands'][0]` instead.

---

## Final Download Run (2026-04-21 09:23–09:24)

- **60/60 files downloaded, 0 failed**
- All files: 53×25 pixels, 0.125° × −0.125°, bounds (−96.75, 40.375, −90.125, 43.5)
- Grid pixel-perfect match to `NLDAS_Iowa_reference_grid.tif`

---

## Pending / Follow-up

- **Exact Iowa clip:** Post-processing step needed to mask downloaded TIFs to the exact Iowa state boundary (use `data/aoi/iowa.geojson`).
- **ET_delta TIFs:** The existing `data/processed/ET_delta/` files are south-up (positive y-scale). If used downstream, they may need to be regenerated with the flip correction applied.
