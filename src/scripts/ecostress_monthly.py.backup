"""
ECOSTRESS Monthly Download Script for AppEEARS API

Downloads ECOSTRESS L3T JET (Evapotranspiration) data for a specified month
or a range of years using the NASA AppEEARS API.

Usage:
    export EARTHDATA_USER="your_username"
    export EARTHDATA_PASS="your_password"

    # Single month
    python ecostress_monthly.py --year 2023 --month 7 --aoi data/aoi/iowa.geojson --outdir data/raw/ECOSTRESS

    # Full year range (2019-2023, all months)
    python ecostress_monthly.py --start-year 2019 --end-year 2023 --aoi data/aoi/iowa.geojson --outdir data/raw/ECOSTRESS

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


def check_task_status(headers: dict, task_id: str) -> str:
    """Check task status without blocking. Returns status string."""
    response = requests.get(f"{API_URL}task/{task_id}", headers=headers, timeout=60)
    response.raise_for_status()
    return response.json().get("status")


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


def run_single_month(args):
    """Run download for a single month (original behavior)."""
    year, month = args.year, args.month
    start_date, end_date = month_dates_mmddyyyy(year, month)

    # Setup output directory (flat structure - all TIFs together)
    outdir = Path(args.outdir)
    manifest_dir = outdir / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"manifest_{year}_{month:02d}.json"
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


def run_batch(args):
    """Run download for all months in a year range.

    Workflow:
      Phase 1 - Submit all tasks (skip months with existing manifests)
      Phase 2 - Poll all tasks until all complete
      Phase 3 - Download all bundles
    """
    start_year = args.start_year
    end_year = args.end_year

    # Build list of (year, month) pairs
    months = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            months.append((year, month))

    print("=" * 60)
    print(f"ECOSTRESS Batch Download")
    print("=" * 60)
    print(f"Product: {PRODUCT}")
    print(f"Layer: {LAYER}")
    print(f"Range: {start_year}-01 to {end_year}-12 ({len(months)} months)")
    print(f"AOI: {args.aoi}")
    print(f"Output: {args.outdir}")
    print("=" * 60)

    # Authenticate
    print("\nAuthenticating with NASA Earthdata...")
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    print("Authentication successful")

    # Load area of interest
    geojson = load_geojson(Path(args.aoi))

    # =========================================================================
    # Phase 1: Submit all tasks
    # =========================================================================
    print(f"\n{'=' * 60}")
    print("PHASE 1: Submitting tasks")
    print(f"{'=' * 60}")

    tasks = {}  # (year, month) -> {"task_id": ..., "outdir": ..., "manifest_path": ...}

    outdir = Path(args.outdir)
    manifest_dir = outdir / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    for year, month in months:
        start_date, end_date = month_dates_mmddyyyy(year, month)
        manifest_path = manifest_dir / f"manifest_{year}_{month:02d}.json"

        task_name = f"ECOSTRESS_ETdaily_{year}_{month:02d}"

        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            task_id = manifest["task_id"]
            print(f"  {year}-{month:02d}: Resuming existing task {task_id}")
        else:
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
            print(f"  {year}-{month:02d}: Submitted -> {task_id}")

        tasks[(year, month)] = {
            "task_id": task_id,
        }

    print(f"\n{len(tasks)} tasks submitted/resumed")

    # =========================================================================
    # Phase 2: Poll all tasks until all complete
    # =========================================================================
    print(f"\n{'=' * 60}")
    print("PHASE 2: Waiting for tasks to complete")
    print(f"{'=' * 60}")

    pending = dict(tasks)  # copy - tasks still being processed
    failed = {}

    while pending:
        done_this_round = []

        for (year, month), info in pending.items():
            task_id = info["task_id"]
            status = check_task_status(headers, task_id)

            if status == "done":
                print(f"  {year}-{month:02d}: DONE")
                done_this_round.append((year, month))
            elif status in ("error", "failed"):
                print(f"  {year}-{month:02d}: FAILED ({status})")
                failed[(year, month)] = info
                done_this_round.append((year, month))

        for key in done_this_round:
            del pending[key]

        if pending:
            remaining = len(pending)
            print(f"\n  {remaining} tasks still processing... waiting {args.poll}s")
            time.sleep(args.poll)

    if failed:
        print(f"\nWARNING: {len(failed)} tasks failed:")
        for (year, month) in failed:
            print(f"  {year}-{month:02d}")

    # =========================================================================
    # Phase 3: Download all completed bundles
    # =========================================================================
    print(f"\n{'=' * 60}")
    print("PHASE 3: Downloading bundles")
    print(f"{'=' * 60}")

    for (year, month), info in tasks.items():
        if (year, month) in failed:
            print(f"\n  {year}-{month:02d}: Skipping (failed)")
            continue

        print(f"\n  {year}-{month:02d}: Downloading...")
        download_bundle(headers, info["task_id"], outdir)

    # Summary
    successful = len(tasks) - len(failed)
    print(f"\n{'=' * 60}")
    print(f"BATCH COMPLETE: {successful}/{len(tasks)} months downloaded")
    if failed:
        print(f"Failed months: {', '.join(f'{y}-{m:02d}' for y, m in failed)}")
    print(f"{'=' * 60}")


def main():
    parser = argparse.ArgumentParser(
        description="Download ECOSTRESS ET data via AppEEARS API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download a single month
    python ecostress_monthly.py --year 2023 --month 7 \\
        --aoi data/aoi/iowa.geojson --outdir data/raw/ECOSTRESS

    # Download all months from 2019 to 2023
    python ecostress_monthly.py --start-year 2019 --end-year 2023 \\
        --aoi data/aoi/iowa.geojson --outdir data/raw/ECOSTRESS

Environment Variables:
    EARTHDATA_USER  - NASA Earthdata username
    EARTHDATA_PASS  - NASA Earthdata password
        """
    )
    # Single-month mode
    parser.add_argument("--year", type=int, help="Year for single-month download")
    parser.add_argument("--month", type=int, help="Month (1-12) for single-month download")

    # Batch mode
    parser.add_argument("--start-year", type=int, help="Start year for batch download")
    parser.add_argument("--end-year", type=int, help="End year for batch download")

    # Common arguments
    parser.add_argument("--aoi", type=str, required=True, help="Path to GeoJSON area of interest")
    parser.add_argument("--outdir", type=str, required=True, help="Base output directory")
    parser.add_argument("--poll", type=int, default=60, help="Polling interval in seconds (default: 60)")
    args = parser.parse_args()

    # Determine mode
    has_single = args.month is not None
    has_batch = args.start_year is not None or args.end_year is not None

    if has_single and has_batch:
        parser.error("Cannot use --month with --start-year/--end-year. Choose one mode.")

    if has_batch:
        if args.start_year is None or args.end_year is None:
            parser.error("Both --start-year and --end-year are required for batch mode.")
        if args.start_year > args.end_year:
            parser.error(f"--start-year ({args.start_year}) must be <= --end-year ({args.end_year})")
        run_batch(args)

    elif has_single:
        if args.year is None:
            args.year = 2023
        if not 1 <= args.month <= 12:
            parser.error(f"Month must be 1-12, got {args.month}")
        run_single_month(args)

    else:
        parser.error("Must specify either --month (single) or --start-year/--end-year (batch).")


if __name__ == "__main__":
    main()
