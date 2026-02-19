#!/usr/bin/env python3
"""
ECOSTRESS Data Download for Iowa (2019-2023)
Runs as batch job on GRIT HPC

Downloads ECOSTRESS L3 JET evapotranspiration data to:
SIF-Analysis/data/raw/ECOSTRESS/

Logs progress to stdout for SLURM capture.
"""

import sys
import subprocess
from pathlib import Path
from datetime import datetime

# =============================================================================
# Install earthaccess if needed
# =============================================================================
print("Setting up earthaccess...", flush=True)

target_dir = '/home/jcoldiron/.local/lib/python3.12/site-packages'

# Try to import, install if missing
try:
    if target_dir not in sys.path:
        sys.path.insert(0, target_dir)
    import earthaccess
    print(f"earthaccess version: {earthaccess.__version__}", flush=True)
except ImportError:
    print("earthaccess not found, installing...", flush=True)
    subprocess.check_call([
        sys.executable, '-m', 'pip', 'install', 
        '--target=' + target_dir,
        'earthaccess'
    ])
    if target_dir not in sys.path:
        sys.path.insert(0, target_dir)
    import earthaccess
    print(f"Installed earthaccess version: {earthaccess.__version__}", flush=True)

# =============================================================================
# Configuration
# =============================================================================

# Temporal range
START_YEAR = 2019
END_YEAR = 2023
temporal_range = (f"{START_YEAR}-01-01", f"{END_YEAR}-12-31")

# Iowa bounding box (west, south, east, north)
iowa_bbox = (-96.64, 40.38, -90.14, 43.50)

# Output path - relative to this script's location (SIF-Analysis/data/raw/ECOSTRESS_JET)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
output_path = PROJECT_ROOT / "data" / "raw" / "ECOSTRESS"
output_path.mkdir(parents=True, exist_ok=True)

# Log file
log_file = output_path / f"download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

def log(message):
    """Print to stdout and write to log file"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{timestamp}] {message}"
    print(log_message, flush=True)
    with open(log_file, 'a') as f:
        f.write(log_message + '\n')

# =============================================================================
# Authenticate
# =============================================================================

log("Authenticating with NASA Earthdata...")
try:
    auth = earthaccess.login()
    log("Authentication successful")
except Exception as e:
    log(f"ERROR: Authentication failed: {e}")
    log("Make sure ~/.netrc is configured with Earthdata credentials")
    sys.exit(1)

# =============================================================================
# Search for ECOSTRESS data
# =============================================================================

log(f"Searching ECOSTRESS L3 JET data for Iowa {START_YEAR}-{END_YEAR}...")
log(f"  Bounding box: {iowa_bbox}")
log(f"  Temporal range: {temporal_range}")

try:
    results = earthaccess.search_data(
        short_name='ECO_L3T_JET',
        version='002',
        bounding_box=iowa_bbox,
        temporal=temporal_range
    )
    log(f"Found {len(results)} granules")
    
    if len(results) == 0:
        log("WARNING: No granules found. Check search parameters.")
        sys.exit(0)
    
    # Count files per granule (each granule has ~12 files)
    if results:
        n_files = len(results[0]['umm']['RelatedUrls'])
        total_files = len(results) * n_files
        log(f"  Files per granule: ~{n_files}")
        log(f"  Estimated total files: ~{total_files}")
    
except Exception as e:
    log(f"ERROR during search: {e}")
    sys.exit(1)

# =============================================================================
# Filter to only ETdaily and cloud mask files
# =============================================================================

# Layers we want to keep - filter by filename suffix
KEEP_LAYERS = ('ETdaily.tif', 'cloud.tif')

log("Filtering granule URLs to ETdaily and cloud mask layers only...")

filtered_urls = []
for granule in results:
    for url in granule.data_links():
        if any(url.endswith(layer) for layer in KEEP_LAYERS):
            filtered_urls.append(url)

log(f"  Total granule files available: ~{len(results) * 12} (estimated)")
log(f"  Filtered to {len(filtered_urls)} files ({', '.join(KEEP_LAYERS)})")

if not filtered_urls:
    log("WARNING: No matching URLs found after filtering. Check KEEP_LAYERS.")
    sys.exit(1)

# =============================================================================
# Download filtered files
# =============================================================================

log(f"\nStarting download to: {output_path}")
log(f"This may take several hours for {len(filtered_urls)} files...")

try:
    downloaded_files = earthaccess.download(
        filtered_urls,
        local_path=str(output_path)
    )

    log(f"\n{'='*60}")
    log(f"DOWNLOAD COMPLETE")
    log(f"{'='*60}")
    log(f"Total files downloaded: {len(downloaded_files)}")
    log(f"Location: {output_path}")

    etdaily_files = [f for f in downloaded_files if 'ETdaily.tif' in str(f)]
    cloud_files = [f for f in downloaded_files if 'cloud.tif' in str(f)]

    log(f"\nFile breakdown:")
    log(f"  ETdaily.tif files: {len(etdaily_files)}")
    log(f"  cloud.tif files:   {len(cloud_files)}")

    # Save file list
    filelist_path = output_path / "downloaded_files.txt"
    with open(filelist_path, 'w') as f:
        for file in sorted(downloaded_files):
            f.write(str(file) + '\n')
    log(f"\nFile list saved to: {filelist_path}")

except Exception as e:
    log(f"ERROR during download: {e}")
    sys.exit(1)

log(f"\nLog file saved to: {log_file}")
log("Script completed successfully!")
