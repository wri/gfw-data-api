import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

from geojson import Feature, FeatureCollection
from pyproj import CRS, Transformer
from shapely.geometry import Polygon
from shapely.ops import unary_union

from errors import GDALError
from gdal_utils import run_gdal_subcommand


def to_4326(crs: CRS, x: float, y: float) -> Tuple[float, float]:
    transformer = Transformer.from_crs(
        crs, CRS.from_epsg(4326), always_xy=True
    )
    return transformer.transform(x, y)


def extract_metadata_from_gdalinfo(gdalinfo_json: Dict[str, Any]) -> Dict[str, Any]:
    """Extract necessary metadata from the gdalinfo JSON output."""
    corner_coordinates = gdalinfo_json["cornerCoordinates"]

    crs: CRS = CRS.from_string(gdalinfo_json["coordinateSystem"]["wkt"])
    metadata = {
        # NOTE: pixetl seems to always write features in tiles.geojson in
        # epsg:4326 coordinates (even when the tiles themselves are
        # epsg:3857). Reproduce that behavior for compatibility. If that
        # ever changes, remove the call to to_4326 here.
        "extent": [
            *to_4326(crs, *corner_coordinates["lowerLeft"]),
            *to_4326(crs, *corner_coordinates["upperRight"]),
        ],
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
                raise RuntimeError(f"Error processing file {file}: {e}")

    tiles_fc = FeatureCollection(features)

    union_geometry = unary_union(polygons)
    extent_fc = FeatureCollection([
        Feature(geometry=union_geometry.__geo_interface__, properties={})
    ])

    return tiles_fc, extent_fc
