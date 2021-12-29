#!/usr/bin/env python

import json
import math
import multiprocessing
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any, Dict, List, Optional, Tuple

import boto3
import rasterio
from errors import SubprocessKilledError
from gfw_pixetl.grids import grid_factory
from gfw_pixetl.pixetl_prep import create_geojsons
from pyproj import CRS, Transformer
from shapely.geometry import Polygon, shape
from shapely.ops import unary_union
from typer import Option, run

AWS_REGION = os.environ.get("AWS_REGION")
AWS_ENDPOINT_URL = os.environ.get("ENDPOINT_URL")  # For boto
NUM_PROCESSES = int(
    os.environ.get(
        "NUM_PROCESSES", os.environ.get("CORES", multiprocessing.cpu_count())
    )
)
GEOTIFF_COMPRESSION = "DEFLATE"
GDAL_GEOTIFF_COMPRESSION = "DEFLATE"


Bounds = Tuple[float, float, float, float]


def replace_inf_nan(number: float, replacement: float) -> float:
    if number == float("inf") or number == float("nan"):
        return replacement
    else:
        return number


def world_bounds(crs: CRS) -> Bounds:
    """Get world bounds for given CRS."""

    from_crs = CRS(4326)

    proj = Transformer.from_crs(from_crs, crs, always_xy=True)

    _left, _bottom, _right, _top = crs.area_of_use.bounds

    # Get World Extent in Source Projection
    # Important: We have to get each top, left, right, bottom separately.
    # We cannot get them using the corner coordinates.
    # For some projections such as Goode (epsg:54052) this would cause strange behavior
    top = proj.transform(0, _top)[1]
    left = proj.transform(_left, 0)[0]
    bottom = proj.transform(0, _bottom)[1]
    right = proj.transform(_right, 0)[0]

    return left, bottom, right, top


def reproject_bounds(bounds: Bounds, src_crs: CRS, crs: CRS) -> Bounds:
    """Reproject src bounds to dst CRT.

    Make sure that coordinates fall within real world coordinates system
    """

    left, bottom, right, top = bounds

    min_lng, min_lat, max_lng, max_lat = world_bounds(crs)

    proj = Transformer.from_crs(CRS.from_user_input(src_crs), crs, always_xy=True)

    reproject_top = replace_inf_nan(round(proj.transform(0, top)[1], 8), max_lat)
    reproject_left = replace_inf_nan(round(proj.transform(left, 0)[0], 8), min_lng)
    reproject_bottom = replace_inf_nan(round(proj.transform(0, bottom)[1], 8), min_lat)
    reproject_right = replace_inf_nan(round(proj.transform(right, 0)[0], 8), max_lng)

    return reproject_left, reproject_bottom, reproject_right, reproject_top


def create_vrt(
    uris: List[str],
    src_file_band: Optional[int] = None,
    vrt_path: str = "all.vrt",
    separate=False,
) -> str:
    """Create VRT file from input URI(s) Adapted from pixetl."""
    input_file_list_string = "\n".join(uris)

    with tempfile.TemporaryDirectory() as temp_dir_name:
        input_list_file_name = os.path.join(temp_dir_name, "input_file_list.txt")

        with open(input_list_file_name, "w") as input_file_list_file:
            input_file_list_file.write(input_file_list_string)

        cmd: List[str] = ["gdalbuildvrt"]

        cmd += ["-input_file_list", input_list_file_name]

        if src_file_band is not None:
            cmd += ["-b", str(src_file_band)]
        if separate:
            cmd += ["-separate"]
        cmd += ["-resolution", "highest"]
        cmd += [vrt_path]

        vrt_process = subprocess.run(cmd, capture_output=True)
        if vrt_process.returncode < 0:
            raise SubprocessKilledError

    return vrt_path


def get_s3_client(aws_region=AWS_REGION, endpoint_url=AWS_ENDPOINT_URL):
    return boto3.client("s3", region_name=aws_region, endpoint_url=endpoint_url)


def get_s3_path_parts(s3url) -> Tuple[str, str]:
    just_path = s3url.split("s3://")[1]
    bucket = just_path.split("/")[0]
    key = "/".join(just_path.split("/")[1:])
    return bucket, key


def from_vsi(file_name: str) -> str:
    """Convert /vsi path to s3 or gs path.

    Stolen from pixetl
    """

    protocols = {"vsis3": "s3", "vsigs": "gs"}

    parts = file_name.split("/")
    try:
        vsi = f"{protocols[parts[1]]}://{'/'.join(parts[2:])}"
    except KeyError:
        raise ValueError(f"Unknown protocol: {parts[1]}")
    return vsi


def get_source_tiles_info(tiles_geojson_uri) -> List[Tuple[str, Any]]:
    s3_client = get_s3_client()
    bucket, key = get_s3_path_parts(tiles_geojson_uri)

    response = s3_client.get_object(Bucket=bucket, Key=key)
    tiles_geojson: Dict[str, Any] = json.loads(response["Body"].read().decode("utf-8"))
    tiles_info: List[Tuple[str, Any]] = [
        (from_vsi(feature["properties"]["name"]), feature["geometry"])
        for feature in tiles_geojson["features"]
    ]
    return tiles_info


def download_tiles(args: Tuple[str, str]) -> str:
    source_tile_uri, dest_dir = args

    local_src_file_path = os.path.join(dest_dir, os.path.basename(source_tile_uri))

    # TODO: Skip download when possible. Use checksum?
    if os.path.isfile(local_src_file_path):
        print(f"Local file {local_src_file_path} already exists, skipping download")
    else:
        bucket, source_key = get_s3_path_parts(source_tile_uri)
        s3_client = get_s3_client()
        print(f"Downloading {source_tile_uri} to {local_src_file_path}")
        s3_client.download_file(bucket, source_key, local_src_file_path)

    return local_src_file_path


def exists_in_s3(target_bucket, target_key):
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(
        Bucket=target_bucket,
        Prefix=target_key,
    )
    for obj in response.get("Contents", []):
        if obj["Key"] == target_key:
            return obj["Size"] > 0


def warp_raster(
    bounds: Bounds, width, height, resampling_method, source_path, target_path
):
    warp_cmd: List[str] = [
        "gdalwarp",
        "-t_srs",
        "epsg:3857",  # TODO: Parameterize?
        "-te",
        f"{bounds[0]}",
        f"{bounds[1]}",
        f"{bounds[2]}",
        f"{bounds[3]}",
        "-ts",
        f"{width}",
        f"{height}",
        "-r",
        resampling_method,
        "-co",
        "TILED=YES",
        "-overwrite",
        source_path,
        target_path,
    ]
    tic = time.perf_counter()
    warp_process = subprocess.run(warp_cmd, capture_output=True)
    toc = time.perf_counter()

    print(f"Warping {source_path} to {target_path} took {toc - tic:0.4f} seconds")

    if warp_process.returncode < 0:
        raise SubprocessKilledError
    elif warp_process.returncode > 0:
        print(warp_process.stderr)

    return toc - tic


def compress_raster(source_path, target_path):
    translate_cmd: List[str] = [
        "gdal_translate",
        "-co",
        "COMPRESS=DEFLATE",
        "-co",
        "TILED=YES",
        "-co",
        "INTERLEAVE=BAND",
        "-co",
        "SPARSE_OK=TRUE",
        source_path,
        target_path,
    ]
    tic = time.perf_counter()
    translate_process = subprocess.run(translate_cmd, capture_output=True)
    toc = time.perf_counter()

    if translate_process.returncode < 0:
        raise SubprocessKilledError
    elif translate_process.returncode > 0:
        print(translate_process.stderr)

    print(f"Compressing {source_path} to {target_path} took {toc - tic:0.4f} seconds")

    return toc - tic


def process_tile(args: Tuple[str, Bounds, str, int, str, str, str]) -> str:
    (
        tile_id,
        tile_bounds,
        resampling_method,
        target_zoom,
        vrt_path,
        target_bucket,
        target_prefix,
    ) = args

    print(f"Beginning processing tile {tile_id}")

    tile_file_name = f"{tile_id}.tif"
    target_key = os.path.join(target_prefix, tile_file_name)

    if exists_in_s3(target_bucket, target_key):
        print(f"Tile {tile_id} already exists in S3, skipping")
        return tile_id

    nb_tiles = max(1, int(2 ** target_zoom / 256)) ** 2
    height = int(2 ** target_zoom * 256 / math.sqrt(nb_tiles))
    width = height

    # Use two temp directories so we can drop the directory with the (gigantic)
    # uncompressed warped file ASAP
    warp_dir = os.path.join(os.path.curdir, "warp_dir")
    os.makedirs(warp_dir, exist_ok=True)
    compressed_dir = os.path.join(os.path.curdir, "compressed_dir")
    os.makedirs(compressed_dir, exist_ok=True)

    local_compressed_tile_path = os.path.join(compressed_dir, tile_file_name)
    local_warped_tile_path = os.path.join(warp_dir, tile_file_name)

    # If the compressed file exists, we know we at least started the gdal_translate
    # step, but don't know if we finished. So remove that file and re-run translate
    # from the warped file (which should exist). If it doesn't exist, run starting
    # with the warp command (with the overwrite command regardless).
    if not os.path.isfile(local_compressed_tile_path):
        warp_raster(
            tile_bounds,
            width,
            height,
            resampling_method,
            vrt_path,
            local_warped_tile_path,
        )
    else:
        os.remove(local_compressed_tile_path)

    compress_raster(local_warped_tile_path, local_compressed_tile_path)

    print(f"Uploading {tile_id} to {target_key}")
    s3_client = get_s3_client()
    s3_client.upload_file(local_compressed_tile_path, target_bucket, target_key)

    os.remove(local_compressed_tile_path)
    os.remove(local_warped_tile_path)

    # FIXME: Still need to create and upload gdal-geotiff

    return tile_id


def resample(
    source_uri: str = Option(..., help="URI of asset's tiles.geojson."),
    resampling_method: str = Option(..., help="Resampling method to use."),
    target_zoom: int = Option(..., help="Target zoom level."),
    target_prefix: str = Option(..., help="Destination S3 prefix."),
):
    print(f"Reprojecting/resampling tiles in {source_uri}")

    src_tiles_info = get_source_tiles_info(source_uri)

    if not src_tiles_info:
        print("No input files! I guess we're good then.")
        return

    # I don't think there's any real benefit to using a temp directory here
    base_dir = os.path.curdir

    source_dir = os.path.join(base_dir, "source_tiles")
    os.makedirs(source_dir, exist_ok=True)

    # First download all the source tiles
    dl_process_args = ((tile_info[0], source_dir) for tile_info in src_tiles_info)

    # Cannot use normal pool here since we run sub-processes
    # https://stackoverflow.com/a/61470465/1410317
    tile_paths: List[str] = list()
    with ProcessPoolExecutor(max_workers=45) as executor:
        for tile_path in executor.map(download_tiles, dl_process_args):
            tile_paths.append(tile_path)
            print(f"Finished downloading source tile to {tile_path}")

    # First create a VRT for each of the input bands, then one overall VRT from these
    # constituent VRTs
    input_vrts: List[str] = list()

    # Create VRTs for each of the bands of each of the input files
    # In this case we can assume each file has the same number of bands
    with rasterio.open(tile_paths[0]) as input_file:
        band_count = input_file.count

    for i in range(band_count):
        vrt_file_path = os.path.join(source_dir, f"source_band_{i+1}.vrt")
        create_vrt(tile_paths, src_file_band=i + 1, vrt_path=vrt_file_path)
        input_vrts.append(vrt_file_path)

    # And finally the overall VRT
    overall_vrt: str = create_vrt(
        input_vrts, None, os.path.join(source_dir, "everything.vrt"), separate=True
    )
    print("VRTs created")

    # Determine which tiles in the target SRS + zoom level intersect with the source
    # bands
    dest_proj_grid = grid_factory(f"zoom_{target_zoom}")
    all_dest_proj_tile_ids = dest_proj_grid.get_tile_ids()

    # Re-project extent into w-m
    # with rasterio.open(overall_vrt) as vrt:
    #     source_crs = CRS.from_epsg(vrt.profile["crs"].to_epsg())

    wm_extent = Polygon()
    for tile_info in src_tiles_info:
        # left, bottom, right, top = reproject_bounds(shape(tile_info[1]).bounds, source_crs, CRS.from_epsg(3857))
        # FIXME?: tile.geojson coords ALWAYS seem to be in EPSG:4326
        left, bottom, right, top = reproject_bounds(
            shape(tile_info[1]).bounds, CRS.from_epsg(4326), CRS.from_epsg(3857)
        )
        wm_extent = unary_union(
            [
                wm_extent,
                Polygon(
                    (
                        (left, top),
                        (right, top),
                        (right, bottom),
                        (left, bottom),
                        (left, top),
                    )
                ),
            ]
        )

    target_tiles: List[Tuple[str, Bounds]] = list()
    for tile_id in all_dest_proj_tile_ids:
        left, bottom, right, top = dest_proj_grid.get_tile_bounds(tile_id)

        tile_geom = Polygon(
            ((left, top), (right, top), (right, bottom), (left, bottom), (left, top))
        )

        if tile_geom.intersects(wm_extent) and not tile_geom.touches(wm_extent):
            print(f"Tile {tile_id} intersects!")
            target_tiles.append((tile_id, (left, bottom, right, top)))

    print(f"Found {len(target_tiles)} intersecting tiles: {target_tiles}")

    bucket, _ = get_s3_path_parts(source_uri)

    warp_process_args = [
        (
            tile_id,
            tile_bounds,
            resampling_method,
            target_zoom,
            overall_vrt,
            bucket,
            target_prefix,
        )
        for tile_id, tile_bounds in target_tiles
    ]

    # Cannot use normal pool here since we run sub-processes
    # https://stackoverflow.com/a/61470465/1410317
    with ProcessPoolExecutor(max_workers=30) as executor:
        for tile_id in executor.map(process_tile, warp_process_args):
            print(f"Finished processing and uploading tile {tile_id}")

    # Now run pixetl_prep.create_geojsons to generate a tiles.geojson and
    # extent.geojson in the target prefix. But that code appends /geotiff
    # to the prefix so remove it first
    # FIXME
    dataset, version, _ = target_prefix.split("/", maxsplit=2)
    create_geojsons_prefix = target_prefix.split(f"{dataset}/{version}/")[1].replace(
        "/geotiff", ""
    )
    print(f"Uploading tiles.geojson to {create_geojsons_prefix}")
    create_geojsons(list(), dataset, version, create_geojsons_prefix, True)


if __name__ == "__main__":
    try:
        run(resample)
    except (BrokenProcessPool, SubprocessKilledError):
        print("One of our subprocesses was killed! Exiting with 137")
        sys.exit(137)
