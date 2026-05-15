#!/usr/bin/env python3
"""
Create State AOI GeoJSON files for Iowa and Nebraska

Two modes:
  1. From a local county-boundaries GeoJSON (default for Iowa, where it already exists)
  2. From GEE TIGER/2018/States dataset (used when local file is unavailable)

Usage:
  python 00_make_aoi.py                        # Iowa from local file (default)
  python 00_make_aoi.py --state nebraska       # Nebraska from GEE TIGER
  python 00_make_aoi.py --all                  # Both states

Outputs:
  data/aoi/iowa.geojson
  data/aoi/nebraska.geojson
"""

import argparse
import json
from pathlib import Path

import geopandas as gpd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
AOI_DIR      = PROJECT_ROOT / 'data' / 'aoi'


def make_aoi_from_file(infile: Path, outfile: Path, state_label: str):
    """Dissolve county boundaries from a local GeoJSON file."""
    if not infile.exists():
        print(f'  ERROR: Input file not found: {infile}')
        return False

    gdf = gpd.read_file(infile)
    if gdf.empty:
        print(f'  ERROR: No features in {infile}')
        return False
    if gdf.crs is None:
        print(f'  ERROR: Input has no CRS. Assign CRS in QGIS then re-export.')
        return False

    gdf = gdf.to_crs('EPSG:4326')
    gdf['_dissolve'] = 1
    dissolved = gdf.dissolve(by='_dissolve').reset_index(drop=True)
    dissolved.to_file(outfile, driver='GeoJSON')

    check = gpd.read_file(outfile)
    print(f'  Wrote {state_label} AOI: {outfile}')
    print(f'    Features: {len(check)} | CRS: {check.crs}')
    print(f'    Bounds: {tuple(round(b, 4) for b in check.total_bounds)}')
    return True


def make_aoi_from_gee(state_name: str, outfile: Path):
    """Download state boundary from GEE TIGER and save as GeoJSON."""
    try:
        import ee
        ee.Initialize(project='ee-jacksoncoldiron')
    except Exception as e:
        print(f'  ERROR: GEE init failed: {e}')
        print('  Install earthengine-api and authenticate first.')
        return False

    print(f'  Downloading {state_name} boundary from GEE TIGER...')
    state_fc = (ee.FeatureCollection('TIGER/2018/States')
                  .filter(ee.Filter.eq('NAME', state_name)))
    geom     = state_fc.geometry().getInfo()

    fc = {
        'type'    : 'FeatureCollection',
        'features': [{'type': 'Feature', 'geometry': geom, 'properties': {'NAME': state_name}}],
    }
    with open(outfile, 'w') as f:
        json.dump(fc, f, indent=2)

    check = gpd.read_file(outfile)
    print(f'  Wrote {state_name} AOI: {outfile}')
    print(f'    Bounds: {tuple(round(b, 4) for b in check.total_bounds)}')
    return True


def main():
    parser = argparse.ArgumentParser(description='Create state AOI GeoJSON files.')
    parser.add_argument('--state', choices=['iowa', 'nebraska', 'all'],
                        default='iowa', help='Which state(s) to create AOI for')
    args = parser.parse_args()

    AOI_DIR.mkdir(parents=True, exist_ok=True)

    states_to_process = ['iowa', 'nebraska'] if args.state == 'all' else [args.state]

    for state in states_to_process:
        outfile = AOI_DIR / f'{state}.geojson'
        if outfile.exists():
            print(f'{state}: already exists at {outfile}')
            continue

        print(f'\nCreating {state} AOI...')
        if state == 'iowa':
            local_counties = PROJECT_ROOT / 'data' / 'raw' / 'Iowa_County_Boundaries.geojson'
            if local_counties.exists():
                make_aoi_from_file(local_counties, outfile, 'Iowa')
            else:
                make_aoi_from_gee('Iowa', outfile)
        elif state == 'nebraska':
            local_counties = PROJECT_ROOT / 'data' / 'raw' / 'Nebraska_County_Boundaries.geojson'
            if local_counties.exists():
                make_aoi_from_file(local_counties, outfile, 'Nebraska')
            else:
                make_aoi_from_gee('Nebraska', outfile)

    print('\nDone.')


if __name__ == '__main__':
    main()