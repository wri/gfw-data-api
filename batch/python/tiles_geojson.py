import json
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Any
from geojson import Feature, FeatureCollection
from shapely.geometry import Polygon
from shapely.ops import unary_union


def run_gdalinfo(file_path: str) -> Dict[str, Any]:
    """Run gdalinfo and parse the output as JSON."""
    try:
        result = subprocess.run(
            ["gdalinfo", "-json", file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True,
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to run gdalinfo on {file_path}: {e.stderr}")


def extract_metadata_from_gdalinfo(gdalinfo_json: Dict[str, Any]) -> Dict[str, Any]:
    """Extract necessary metadata from the gdalinfo JSON output."""
    corner_coordinates = gdalinfo_json["cornerCoordinates"]
    geo_transform = gdalinfo_json["geoTransform"]

    bands = [
        {
            "data_type": band.get("type", None),
            "no_data": band.get("noDataValue", None),
            "nbits": band.get("metadata", {}).get("IMAGE_STRUCTURE", {}).get("NBITS", None),
            "blockxsize": band.get("block", [None])[0],
            "blockysize": band.get("block", [None])[1],
            "stats": {
                "min": band.get("minimum"),
                "max": band.get("maximum"),
                "mean": band.get("mean"),
                "std_dev": band.get("stdDev"),
            } if "minimum" in band and "maximum" in band else None,
            "histogram": band.get("histogram", None),
        }
        for band in gdalinfo_json.get("bands", [])
    ]

    metadata = {
        "extent": [
            corner_coordinates["lowerLeft"][0],
            corner_coordinates["lowerLeft"][1],
            corner_coordinates["upperRight"][0],
            corner_coordinates["upperRight"][1],
        ],
        "width": gdalinfo_json["size"][0],
        "height": gdalinfo_json["size"][1],
        "pixelxsize": geo_transform[1],
        "pixelysize": abs(geo_transform[5]),
        "crs": gdalinfo_json["coordinateSystem"]["wkt"],
        "driver": gdalinfo_json.get("driverShortName", None),
        "compression": gdalinfo_json.get("metadata", {}).get("IMAGE_STRUCTURE", {}).get("COMPRESSION", None),
        "bands": bands,
        "name": gdalinfo_json["description"],
    }

    return metadata


def process_file(file_path: str) -> Dict[str, Any]:
    """Run gdalinfo and extract metadata for a single file."""
    print(f"Running gdalinfo on {file_path}")
    gdalinfo_json = run_gdalinfo(file_path)
    return extract_metadata_from_gdalinfo(gdalinfo_json)


def generate_geojson_parallel(geo_tiffs: List[str], tiles_output: str, extent_output: str, max_workers: int = None):
    """Generate tiles.geojson and extent.geojson files."""
    features = []
    polygons = []
    errors = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(process_file, file): file for file in geo_tiffs}
        for future in as_completed(future_to_file):
            file = future_to_file[future]
            try:
                metadata = future.result()
                extent = metadata["extent"]

                # Create a Polygon from the extent
                polygon_coords = [
                    [extent[0], extent[1]],
                    [extent[0], extent[3]],
                    [extent[2], extent[3]],
                    [extent[2], extent[1]],
                    [extent[0], extent[1]],
                ]
                polygon = Polygon(polygon_coords)

                # Add to GeoJSON features
                feature = Feature(geometry=polygon.__geo_interface__, properties=metadata)
                features.append(feature)

                # Collect for union
                polygons.append(polygon)
            except Exception as e:
                print(f"Error processing file {file}: {e}")
                errors.append(f"File {file}: {e}")

    if errors:
        raise RuntimeError(f"Failed to process the following files:\n" + "\n".join(errors))

    # Write tiles.geojson
    tiles_fc = FeatureCollection(features)
    tiles_txt = json.dumps(tiles_fc, indent=2)
    print(f"tiles.geojson:\n", tiles_txt)

    with open(tiles_output, "w") as f:
        print(tiles_txt, file=f)
    print(f"GeoJSON written to {tiles_output}")

    # Create and write extent.geojson
    union_geometry = unary_union(polygons)
    extent_fc = FeatureCollection([
        Feature(geometry=union_geometry.__geo_interface__, properties={})
    ])
    extent_txt = json.dumps(extent_fc, indent=2)
    print(f"extent.geojson:\n", extent_txt)

    with open(extent_output, "w") as f:
        print(extent_txt, file=f)
    print(f"GeoJSON written to {extent_output}")
