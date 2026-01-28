import argparse
from pathlib import Path

import geopandas as gpd


def main():
    parser = argparse.ArgumentParser(
        description="Create a dissolved Iowa AOI GeoJSON from county boundaries."
    )
    parser.add_argument(
        "--infile",
        type=str,
        default="data/raw/Iowa_County_Boundaries.geojson",
        help="Input county boundaries GeoJSON",
    )
    parser.add_argument(
        "--outfile",
        type=str,
        default="data/aoi/iowa.geojson",
        help="Output dissolved AOI GeoJSON",
    )
    args = parser.parse_args()

    infile = Path(args.infile)
    outfile = Path(args.outfile)
    outfile.parent.mkdir(parents=True, exist_ok=True)

    # Load
    gdf = gpd.read_file(infile)

    if gdf.empty:
        raise ValueError(f"No features found in: {infile}")

    # Ensure WGS84 lat/lon
    # If CRS is missing, you must define it (common problem). We'll error loudly.
    if gdf.crs is None:
        raise ValueError(
            "Input file has no CRS defined. Open it in QGIS/ArcGIS and assign the correct CRS, "
            "then re-export OR tell me what CRS it should be and we’ll set it here."
        )

    gdf = gdf.to_crs("EPSG:4326")

    # Dissolve all counties into a single Iowa geometry
    # (dissolve by constant -> one row)
    gdf["_dissolve"] = 1
    iowa = gdf.dissolve(by="_dissolve").reset_index(drop=True)

    # Write GeoJSON
    iowa.to_file(outfile, driver="GeoJSON")

    # Sanity checks
    iowa_check = gpd.read_file(outfile)
    print("✅ Wrote AOI to:", outfile.resolve())
    print("   Features:", len(iowa_check))
    print("   CRS:", iowa_check.crs)
    print("   Bounds (minx, miny, maxx, maxy):", tuple(iowa_check.total_bounds))


if __name__ == "__main__":
    main()