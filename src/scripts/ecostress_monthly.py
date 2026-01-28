"""
ECOSTRESS Monthly Download Script for AppEEARS API

Downloads ECOSTRESS L3T JET (Evapotranspiration) data for a specified month
and area of interest using the NASA AppEEARS API.

Usage:
    export EARTHDATA_USER="your_username"
    export EARTHDATA_PASS="your_password"
    python ecostress_monthly.py --year 2023 --month 7 --aoi data/aoi/iowa.geojson --outdir data/raw/ECOSTRESS

Reference: AppEEARS API Area Request notebook (NASA)
"""

import os
import json
import time
import argparse
import calendar
from pathlib import Path
import requests

# API Configuration (matches reference notebook)
API_URL = "https://appeears.earthdatacloud.nasa.gov/api/"  # Note: trailing slash per reference
PRODUCT = "ECO_L3T_JET.002"  # ECOSTRESS L3 Tiled Evapotranspiration (JET algorithm)
LAYER = "ETdaily"


def get_token() -> str:
    """Authenticate with NASA Earthdata and return bearer token."""
    user = os.environ.get("EARTHDATA_USER")
    pwd = os.environ.get("EARTHDATA_PASS")

    if not user or not pwd:
        raise ValueError(
            "Missing credentials. Set EARTHDATA_USER and EARTHDATA_PASS environment variables."
        )

    response = requests.post(f"{API_URL}login", auth=(user, pwd), timeout=60)
    response.raise_for_status()
    return response.json()["token"]


def load_geojson(path: Path) -> dict:
    """Load GeoJSON file and format for AppEEARS API."""
    with open(path, "r") as f:
        geojson = json.load(f)

    # AppEEARS expects a FeatureCollection or Feature with geometry
    # If loading a full GeoJSON file, it should work directly
    return geojson


def submit_task(
    headers: dict,
    geojson: dict,
    start_date: str,
    end_date: str,
    task_name: str
) -> str:
    """
    Submit an area request task to AppEEARS.

    Args:
        headers: Authorization headers with bearer token
        geojson: GeoJSON dict defining area of interest
        start_date: Start date in MM-DD-YYYY format
        end_date: End date in MM-DD-YYYY format
        task_name: User-defined task name

    Returns:
        task_id: The AppEEARS task ID for tracking
    """
    # Task payload structure per reference notebook (Cell 54)
    task = {
        "task_type": "area",
        "task_name": task_name,
        "params": {
            "dates": [
                {
                    "startDate": start_date,
                    "endDate": end_date
                }
            ],
            "layers": [
                {
                    "product": PRODUCT,
                    "layer": LAYER
                }
            ],
            "output": {
                "format": {"type": "geotiff"},
                "projection": "geographic"
            },
            "geo": geojson
        }
    }

    response = requests.post(f"{API_URL}task", json=task, headers=headers, timeout=60)

    if not response.ok:
        print(f"ERROR - Status: {response.status_code}")
        print(f"Response: {response.text}")
    response.raise_for_status()

    return response.json()["task_id"]


def poll_until_done(headers: dict, task_id: str, poll_seconds: int = 60) -> None:
    """
    Poll task status until completion or error.

    Reference: Cell 64 in AppEEARS notebook
    """
    start_time = time.time()

    while True:
        response = requests.get(f"{API_URL}task/{task_id}", headers=headers, timeout=60)
        response.raise_for_status()

        status = response.json().get("status")
        elapsed = time.time() - start_time
        print(f"Task status: {status} (elapsed: {elapsed/60:.1f} min)")

        if status == "done":
            return
        if status in ("error", "failed"):
            raise RuntimeError(f"Task failed with status '{status}'. Check AppEEARS UI for details.")

        time.sleep(poll_seconds)


def download_bundle(headers: dict, task_id: str, outdir: Path) -> None:
    """
    Download all files from a completed task bundle.

    Reference: Cells 68-72 in AppEEARS notebook
    """
    outdir.mkdir(parents=True, exist_ok=True)

    # Get bundle contents
    response = requests.get(f"{API_URL}bundle/{task_id}", headers=headers, timeout=60)
    response.raise_for_status()
    bundle = response.json()

    files = bundle.get("files", [])
    print(f"Bundle contains {len(files)} files")

    # Build file_id -> file_name mapping (per reference notebook Cell 70)
    for fmeta in files:
        file_id = fmeta["file_id"]
        file_name = fmeta["file_name"]

        # Handle nested paths in file_name (e.g., "subfolder/file.tif")
        if "/" in file_name:
            fname = file_name.split("/")[-1]
        else:
            fname = file_name

        outpath = outdir / fname

        # Skip if already downloaded
        if outpath.exists() and outpath.stat().st_size > 0:
            print(f"Skipping (exists): {fname}")
            continue

        # Download using file_id in URL (per reference notebook Cell 72)
        print(f"Downloading: {fname}")
        dl_url = f"{API_URL}bundle/{task_id}/{file_id}"
        dl_response = requests.get(
            dl_url,
            headers=headers,
            stream=True,
            allow_redirects=True,
            timeout=300
        )
        dl_response.raise_for_status()

        # Write in chunks for large files
        with open(outpath, "wb") as f:
            for chunk in dl_response.iter_content(chunk_size=8192):
                f.write(chunk)

def month_dates_mmddyyyy(year: int, month: int) -> tuple[str, str]:
    """
    Generate start and end dates for a given month in MM-DD-YYYY format.

    This is the date format required by AppEEARS API (per reference notebook Cell 52).
    """
    last_day = calendar.monthrange(year, month)[1]
    start = f"{month:02d}-01-{year}"
    end = f"{month:02d}-{last_day:02d}-{year}"
    return start, end


def main():
    parser = argparse.ArgumentParser(
        description="Download ECOSTRESS ET data for a month via AppEEARS API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download July 2023 data for Iowa
    python ecostress_monthly.py --year 2023 --month 7 \\
        --aoi data/aoi/iowa.geojson --outdir data/raw/ECOSTRESS

    # Resume a partially completed download
    python ecostress_monthly.py --year 2023 --month 7 \\
        --aoi data/aoi/iowa.geojson --outdir data/raw/ECOSTRESS

Environment Variables:
    EARTHDATA_USER  - NASA Earthdata username
    EARTHDATA_PASS  - NASA Earthdata password
        """
    )
    parser.add_argument("--year", type=int, default=2023, help="Year (default: 2023)")
    parser.add_argument("--month", type=int, required=True, help="Month (1-12)")
    parser.add_argument("--aoi", type=str, required=True, help="Path to GeoJSON area of interest")
    parser.add_argument("--outdir", type=str, required=True, help="Base output directory")
    parser.add_argument("--poll", type=int, default=60, help="Polling interval in seconds (default: 60)")
    args = parser.parse_args()

    # Validate month
    if not 1 <= args.month <= 12:
        parser.error(f"Month must be 1-12, got {args.month}")

    year, month = args.year, args.month
    start_date, end_date = month_dates_mmddyyyy(year, month)

    # Setup output directory with partitioning
    outdir = Path(args.outdir) / f"year={year}" / f"month={month:02d}"
    manifest_path = outdir / "manifest.json"
    outdir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(f"ECOSTRESS Monthly Download")
    print("=" * 60)
    print(f"Product: {PRODUCT}")
    print(f"Layer: {LAYER}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"AOI: {args.aoi}")
    print(f"Output: {outdir}")
    print("=" * 60)

    # Authenticate
    print("\nAuthenticating with NASA Earthdata...")
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    print("Authentication successful")

    # Load area of interest
    geojson = load_geojson(Path(args.aoi))

    task_name = f"ECOSTRESS_ETdaily_{year}_{month:02d}"

    # Resume if manifest exists (allows restarting interrupted downloads)
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        task_id = manifest["task_id"]
        print(f"\nResuming existing task: {task_id}")
    else:
        print(f"\nSubmitting new task: {task_name}")
        task_id = submit_task(headers, geojson, start_date, end_date, task_name)
        manifest = {
            "task_id": task_id,
            "task_name": task_name,
            "year": year,
            "month": month,
            "start_date": start_date,
            "end_date": end_date,
            "product": PRODUCT,
            "layer": LAYER
        }
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"Task submitted: {task_id}")
        print(f"Manifest saved: {manifest_path}")

    # Wait for processing
    print("\nWaiting for task to complete...")
    poll_until_done(headers, task_id, poll_seconds=args.poll)

    # Download results
    print("\nDownloading bundle...")
    download_bundle(headers, task_id, outdir)

    print(f"\nDownload complete: {outdir}")


if __name__ == "__main__":
    main()