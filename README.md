# SIF Analysis - Iowa Agricultural Areas

This repository contains analysis of Solar-Induced Chlorophyll Fluorescence (SIF) data for Iowa agricultural areas, focusing on seasonal patterns and drought impacts.

## Project Structure

```
SIF/
├── data/                     # Data directory
│   ├── raw/                  # Raw data files
│   │   ├── SIF_OCO2_005_v11r/    # SIF netCDF files
│   │   └── Cornbelt_annual_CDL/  # CDL raster files
│   ├── processed/            # Processed data
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

## Author

- **Jackson Coldiron**, UCSB Master's Student
- **Advisors:** Sophie Ruehr (Stanford) & Zoe Pierrat (UCSB)

## Data Citation

The data used in the analysis was generated using the workflow outlined in:

Yu, L., Wen, J., Chang, C. Y., Frankenberg, C., & Sun, Y. (2019). High-resolution global contiguous SIF of OCO-2. Geophysical Research Letters, 46(3), 1449–1458. https://doi.org/10.1029/2018GL081109
