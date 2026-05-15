#!/usr/bin/env python3
"""
USDA NASS QuickStats Crop Yield Download — Iowa and Nebraska (2015-2024)

Downloads annual corn and soybean yield data from the USDA NASS QuickStats API
for Iowa and Nebraska. Data includes:
  - Corn for grain: yield (bu/acre) at state level
  - Soybeans: yield (bu/acre) at state level

API documentation: https://quickstats.nass.usda.gov/api

REQUIREMENTS:
  - NASS API key (free, request at https://quickstats.nass.usda.gov/api)
  - Set NASS_API_KEY environment variable or edit API_KEY below
  - requests library

OUTPUTS:
  data/raw/yield/corn_yield_iowa_nebraska_2015_2024.csv
  data/raw/yield/soy_yield_iowa_nebraska_2015_2024.csv
  data/raw/yield/crop_yield_combined.csv  (both crops, both states)

DATA NOTES:
  - NASS yields are annual, reported for the calendar year
  - Yield is in bushels per acre (bu/acre)
  - To convert to tonnes/hectare: corn × 0.06277, soy × 0.06725
  - State-level data: one value per state per year
  - County-level data also available (set AGG_LEVEL = 'COUNTY')
"""

import os
import sys
import csv
import json
import time
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError

# =============================================================================
# Configuration
# =============================================================================

# Get API key from environment variable or hardcode here
API_KEY = os.environ.get('NASS_API_KEY', 'YOUR_API_KEY_HERE')

if API_KEY == 'YOUR_API_KEY_HERE':
    print('ERROR: NASS API key not set.')
    print('  1. Request a free key at: https://quickstats.nass.usda.gov/api')
    print('  2. Set it: export NASS_API_KEY=your_key_here')
    print('  3. Or edit API_KEY in this script directly.')
    sys.exit(1)

API_BASE = 'https://quickstats.nass.usda.gov/api/api_GET/'

STATES     = ['IOWA', 'NEBRASKA']
YEARS      = list(range(2015, 2025))   # 2015–2024
AGG_LEVEL  = 'STATE'   # 'STATE' or 'COUNTY'

COMMODITIES = {
    'CORN'    : {'commodity': 'CORN', 'util_practice': 'GRAIN',  'stat_cat': 'YIELD'},
    'SOYBEANS': {'commodity': 'SOYBEANS', 'util_practice': 'ALL UTILIZATION PRACTICES', 'stat_cat': 'YIELD'},
}

PROJECT_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR   = PROJECT_ROOT / 'data' / 'raw' / 'yield'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# API Query Function
# =============================================================================

def query_nass(params: dict) -> list[dict]:
    """Query the NASS QuickStats API and return list of records."""
    params['key']    = API_KEY
    params['format'] = 'JSON'

    url = API_BASE + '?' + urlencode(params)

    try:
        with urlopen(url, timeout=30) as r:
            data = json.loads(r.read().decode())
    except HTTPError as e:
        print(f'  HTTP error: {e.code} {e.reason}')
        return []

    if 'data' not in data:
        msg = data.get('error', ['Unknown error'])[0] if 'error' in data else 'No data returned'
        print(f'  API error: {msg}')
        return []

    return data['data']


# =============================================================================
# Download loop
# =============================================================================

all_records = []

for crop_label, crop_params in COMMODITIES.items():
    for state in STATES:
        print(f'Querying {crop_label} yield — {state}...')

        params = {
            'source_desc'      : 'SURVEY',
            'sector_desc'      : 'CROPS',
            'group_desc'       : 'FIELD CROPS',
            'commodity_desc'   : crop_params['commodity'],
            'util_practice_desc': crop_params['util_practice'],
            'statisticcat_desc': crop_params['stat_cat'],
            'unit_desc'        : 'BU / ACRE',
            'agg_level_desc'   : AGG_LEVEL,
            'state_name'       : state,
            'year__GE'         : str(min(YEARS)),
            'year__LE'         : str(max(YEARS)),
        }

        records = query_nass(params)

        if records:
            # Filter to our exact years (API may return adjacent years)
            filtered = [r for r in records if int(r['year']) in YEARS]
            print(f'  {len(filtered)} records for {state} {crop_label} ({min(YEARS)}–{max(YEARS)})')
            all_records.extend(filtered)
        else:
            print(f'  No records found.')

        time.sleep(0.5)   # polite pause between API calls

# =============================================================================
# Save outputs
# =============================================================================

if not all_records:
    print('\nNo data downloaded. Check API key and parameters.')
    sys.exit(1)

# Identify key columns (NASS returns many metadata columns — keep the essentials)
KEY_COLS = [
    'year', 'state_name', 'commodity_desc', 'statisticcat_desc',
    'unit_desc', 'agg_level_desc', 'Value',
    'county_name', 'county_ansi',   # empty for state-level, populated for county
    'util_practice_desc', 'prodn_practice_desc',
]

def safe_row(record):
    return {col: record.get(col, '') for col in KEY_COLS}

# Combined CSV (all crops, all states)
combined_path = OUTPUT_DIR / 'crop_yield_combined.csv'
with open(combined_path, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=KEY_COLS, extrasaction='ignore')
    writer.writeheader()
    for r in all_records:
        writer.writerow(safe_row(r))

print(f'\nSaved combined: {combined_path}')
print(f'Total records: {len(all_records)}')

# Per-commodity CSVs
for crop_label in COMMODITIES:
    crop_records = [r for r in all_records
                    if r.get('commodity_desc', '').upper() == crop_label]
    if not crop_records:
        continue
    out_path = OUTPUT_DIR / f'{crop_label.lower()}_yield_iowa_nebraska_{min(YEARS)}_{max(YEARS)}.csv'
    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=KEY_COLS, extrasaction='ignore')
        writer.writeheader()
        for r in crop_records:
            writer.writerow(safe_row(r))
    print(f'Saved {crop_label}: {out_path}  ({len(crop_records)} records)')

# Print summary table
print('\n--- Yield Summary (bu/acre) ---')
print(f'{"State":<12} {"Crop":<12} {"Year":<6} {"Yield":>8}')
print('-' * 42)
for r in sorted(all_records, key=lambda x: (x['state_name'], x['commodity_desc'], x['year'])):
    try:
        val = float(r['Value'].replace(',', ''))
        print(f"{r['state_name']:<12} {r['commodity_desc']:<12} {r['year']:<6} {val:>8.1f}")
    except (ValueError, KeyError):
        pass

print(f'\nData saved to: {OUTPUT_DIR}')
print('\nNOTE: These are state-level annual averages.')
print('For analysis integration, join to the annual CDL crop-type fractions by year and state.')
