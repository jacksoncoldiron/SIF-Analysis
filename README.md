# SIF Analysis - Iowa Agricultural Areas

This repository contains analysis of Solar-Induced Fluorescence (SIF) data for Iowa agricultural areas, focusing on seasonal patterns and drought impacts.

## Project Structure

```
SIF/
├── data/                     # Data directory
│   ├── raw/                  # Raw data files
│   │   ├── SIF_OCO2_005_v11r/    # SIF netCDF files
│   │   └── Cornbelt_annual_CDL/  # CDL raster files
│   ├── processed/            # Processed data
│   └── outputs/              # Analysis outputs
├── src/                      # Source code
│   └── sif_dataviz.ipynb     # Main analysis notebook
├── figures/                  # Generated plots and visualizations
└── README.md                 # This file
```

## Analysis Overview

- **Geographic Focus**: Iowa agricultural areas
- **Time Period**: 2014-2024
- **Data Sources**: 
  - OCO-2 SIF data (NASA)
  - Cropland Data Layer (CDL) for agricultural masking
- **Key Analyses**:
  - Seasonal SIF cycles for agricultural areas
  - Morning vs evening diurnal patterns
  - Drought impact analysis
  - Monthly spatial statistics

## Getting Started

1. Clone the repository
2. Open `src/sif_dataviz.ipynb` in Jupyter Lab/Notebook
3. Run cells sequentially to reproduce the analysis
4. Check the `figures/` directory for generated plots

## Data Requirements

The large data files (netCDF, TIF) are excluded from the repository due to size constraints but should be placed in the `data/raw/` directory structure as shown above.

## Tools Used

- **Python**: xarray, rasterio, pandas, matplotlib
- **Data**: SIF (satellite), CDL (agricultural classification)
- **Visualization**: Matplotlib plots with seasonal cycle analysis
