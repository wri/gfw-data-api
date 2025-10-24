import json
import math
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

from geojson import Feature, FeatureCollection
from pyproj import CRS, Transformer
from shapely.geometry import Polygon
from shapely.ops import unary_union

from errors import GDALError
from gdal_utils import from_gdal_data_type, run_gdal_subcommand


def to_4326(crs: CRS, x: float, y: float) -> Tuple[float, float]:
    transformer = Transformer.from_crs(
        crs, CRS.from_epsg(4326), always_xy=True
    )
    return transformer.transform(x, y)


def extract_metadata_from_gdalinfo(gdalinfo_json: Dict[str, Any]) -> Dict[str, Any]:
    """Extract necessary metadata from the gdalinfo JSON output."""
    wgs84Extent = gdalinfo_json["wgs84Extent"]["coordinates"][0]
    geo_transform = gdalinfo_json["geoTransform"]

    bands = [
        {
            "data_type": (
                from_gdal_data_type(band.get("type"))
                if band.get("type") is not None
                else None
            ),
            "no_data": (
                "nan" if (
                    band.get("noDataValue", None) is not None
                    and math.isnan(band.get("noDataValue"))
                )
                else band.get("noDataValue", None)
            ),
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
        # wgs84Extent is in decimal degrees, not meters.
        "extent": [
            wgs84Extent[0][0],  # left of upperleft
            wgs84Extent[2][1],  # bottom of lowerRight
            wgs84Extent[2][0],  # right of lowerRight
            wgs84Extent[0][1]   # top of upperLeft
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
    try:
        stdout,stderr = run_gdal_subcommand(
            ["gdalinfo", "-json", file_path],
        )
    except GDALError as e:
        raise RuntimeError(f"Failed to run gdalinfo on {file_path}: {e}")

    gdalinfo_json: Dict = json.loads(stdout)
    return extract_metadata_from_gdalinfo(gdalinfo_json)


def generate_geojsons(
    geotiffs: List[str],
    max_workers: int = None
) -> Tuple[FeatureCollection, FeatureCollection]:
    """Generate tiles.geojson and extent.geojson files."""
    features = []
    polygons = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(process_file, file): file for file in geotiffs}
        for future in as_completed(future_to_file):
            file = future_to_file[future]
            try:
                metadata = future.result()
                extent = metadata["extent"]
                # Create a Polygon from the extent
                polygon_coords = [
                    [extent[0], extent[3]],  # left/top
                    [extent[2], extent[3]],  # right/top
                    [extent[2], extent[1]],  # right/bottom
                    [extent[0], extent[1]],  # left/bottom
                    [extent[0], extent[3]],  # left/top
                ]
                polygon = Polygon(polygon_coords)

                # Add to GeoJSON features
                feature = Feature(geometry=polygon.__geo_interface__, properties=metadata)
                features.append(feature)

                # Collect for union
                polygons.append(polygon)
            except Exception as e:
                raise RuntimeError(f"Error processing file {file}: {e}")

    tiles_fc = FeatureCollection(features)

    union_geometry = unary_union(polygons)
    extent_fc = FeatureCollection([
        Feature(geometry=union_geometry.__geo_interface__, properties={})
    ])

    return tiles_fc, extent_fc
