#!/usr/bin/env python

import json
import logging
import math
import multiprocessing
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from logging.handlers import QueueHandler
from multiprocessing import Queue
from typing import Any, Callable, Dict, List, Optional, Tuple

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
NUM_DL_PROCS = max(
    int(int(os.environ.get("CORES", multiprocessing.cpu_count())) / 3), 1
)
NUM_TRANSFORM_PROCS = max(
    int(int(os.environ.get("CORES", multiprocessing.cpu_count())) / 3.5), 1
)
GEOTIFF_COMPRESSION = "DEFLATE"

Bounds = Tuple[float, float, float, float]


def replace_inf_nan(number: float, replacement: float) -> float:
    if number == float("inf") or number == float("nan"):
        return replacement
    else:
        return number


def world_bounds(crs: CRS) -> Bounds:
    """Get the world bounds for a given CRS.

    Taken from pixetl
    """

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
    """Reproject source bounds from source CRS to destination CRS.

    Make sure that coordinates fall within real world coordinates system
    Taken from pixetl
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
    """Create a VRT file from input URI(s)

    Adapted from pixetl.
    """
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
        if vrt_process.returncode > 0:
            # TODO: Log output
            raise Exception("Error creating VRT!")

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

    Taken from pixetl
    """

    protocols = {"vsis3": "s3", "vsigs": "gs"}

    parts = file_name.split("/")
    try:
        vsi = f"{protocols[parts[1]]}://{'/'.join(parts[2:])}"
    except KeyError:
        raise ValueError(f"Unknown protocol: {parts[1]}")
    return vsi


def get_source_tiles_info(tiles_geojson_uri) -> List[Tuple[str, Any]]:
    """Returns a list of tuples, each of which is the URL of a file referenced
    in the target tiles.geojson along with its GeoJSON feature."""
    s3_client = get_s3_client()
    bucket, key = get_s3_path_parts(tiles_geojson_uri)

    response = s3_client.get_object(Bucket=bucket, Key=key)
    tiles_geojson: Dict[str, Any] = json.loads(response["Body"].read().decode("utf-8"))
    tiles_info: List[Tuple[str, Any]] = [
        (from_vsi(feature["properties"]["name"]), feature["geometry"])
        for feature in tiles_geojson["features"]
    ]
    return tiles_info


def listener_configurer():
    root = logging.getLogger()
    h = logging.StreamHandler(stream=sys.stdout)
    # f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
    # h.setFormatter(f)
    root.addHandler(h)


def log_listener(queue, configurer):
    configurer()
    while True:
        try:
            record = queue.get()
            if (
                record is None
            ):  # We send this as a sentinel to tell the listener to quit.
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        except Exception:
            import traceback

            print("Encountered a problem in the log listener!", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            raise


def log_client_configurer(queue):
    h = QueueHandler(queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(logging.INFO)


def download_tile(args: Tuple[str, str, Queue, Callable]) -> str:
    """Download the file at the first item of the tuple to the destination
    directory specified by the second item."""
    source_tile_uri, dest_dir, q, q_configurer = args

    q_configurer(q)
    logger = logging.getLogger("download_tiles")

    local_src_file_path = os.path.join(dest_dir, os.path.basename(source_tile_uri))

    # TODO: Use checksum to detect partial downloads?
    if os.path.isfile(local_src_file_path):
        logger.log(
            logging.INFO,
            f"Local file {local_src_file_path} already exists, skipping download",
        )
    else:
        bucket, source_key = get_s3_path_parts(source_tile_uri)
        s3_client = get_s3_client()
        logger.log(
            logging.INFO, f"Downloading {source_tile_uri} to {local_src_file_path}"
        )
        s3_client.download_file(bucket, source_key, local_src_file_path)

    return local_src_file_path


def exists_in_s3(target_bucket, target_key):
    """Returns whether or not target_key exists in the target bucket."""
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(
        Bucket=target_bucket,
        Prefix=target_key,
    )
    for obj in response.get("Contents", []):
        if obj["Key"] == target_key:
            return obj["Size"] > 0


def warp_raster(
    bounds: Bounds, width, height, resampling_method, source_path, target_path, logger
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

    logger.log(
        logging.INFO,
        f"Warping {source_path} to {target_path} took {toc - tic:0.4f} seconds",
    )

    if warp_process.returncode < 0:
        raise SubprocessKilledError
    elif warp_process.returncode > 0:
        logger.log(logging.ERROR, warp_process.stderr)
        raise Exception(f"Warping {source_path} failed")

    return toc - tic


def compress_raster(source_path, target_path, logger):
    translate_cmd: List[str] = [
        "gdal_translate",
        "-co",
        f"COMPRESS={GEOTIFF_COMPRESSION}",
        "-co",
        "TILED=YES",
        source_path,
        target_path,
    ]
    tic = time.perf_counter()
    translate_process = subprocess.run(translate_cmd, capture_output=True)
    toc = time.perf_counter()

    if translate_process.returncode < 0:
        raise SubprocessKilledError
    elif translate_process.returncode > 0:
        logger.log(logging.ERROR, translate_process.stderr)
        raise Exception(f"Compressing {source_path} failed")

    logger.log(
        logging.INFO,
        f"Compressing {source_path} to {target_path} took {toc - tic:0.4f} seconds",
    )

    return toc - tic


def process_tile(
    args: Tuple[str, Bounds, str, int, str, str, str, Queue, Callable]
) -> str:
    (
        tile_id,
        tile_bounds,
        resampling_method,
        target_zoom,
        vrt_path,
        target_bucket,
        target_prefix,
        q,
        q_configurer,
    ) = args

    q_configurer(q)
    logger = logging.getLogger("process_tile")
    logger.log(logging.INFO, f"Beginning processing tile {tile_id}")

    tile_file_name = f"{tile_id}.tif"
    target_geotiff_key = os.path.join(target_prefix, "geotiff", tile_file_name)
    target_gdal_geotiff_key = os.path.join(
        target_prefix, "gdal-geotiff", tile_file_name
    )

    if exists_in_s3(target_bucket, target_geotiff_key) and exists_in_s3(
        target_bucket, target_gdal_geotiff_key
    ):
        logger.log(logging.INFO, f"Tile {tile_id} already exists in S3, skipping")
        return tile_id

    nb_tiles = max(1, int(2 ** target_zoom / 256)) ** 2
    height = int(2 ** target_zoom * 256 / math.sqrt(nb_tiles))
    width = height

    warp_dir = os.path.join(os.path.curdir, "warped")
    os.makedirs(warp_dir, exist_ok=True)
    warped_tile_path = os.path.join(warp_dir, tile_file_name)

    compressed_dir = os.path.join(os.path.curdir, "compressed")
    os.makedirs(compressed_dir, exist_ok=True)
    compressed_tile_path = os.path.join(compressed_dir, tile_file_name)

    # If the compressed file exists, we know we at least started the gdal_translate
    # step, but don't know if we finished. So remove that file and re-run translate
    # from the warped file (which should exist). If the compressed file doesn't exist
    # at all, run starting with the warp command (with the overwrite option
    # regardless).
    if os.path.isfile(compressed_tile_path):
        os.remove(compressed_tile_path)
    else:
        warp_raster(
            tile_bounds,
            width,
            height,
            resampling_method,
            vrt_path,
            warped_tile_path,
            logger,
        )

    compress_raster(warped_tile_path, compressed_tile_path, logger)

    logger.log(logging.INFO, f"Uploading {tile_id} to S3...")
    s3_client = get_s3_client()
    s3_client.upload_file(compressed_tile_path, target_bucket, target_geotiff_key)
    logger.log(logging.INFO, f"Finished uploading {tile_id} to S3")

    os.remove(compressed_tile_path)
    os.remove(warped_tile_path)

    return tile_id


def resample(
    dataset: str = Option(..., help="Dataset name."),
    version: str = Option(..., help="Version string."),
    source_uri: str = Option(..., help="URI of asset's tiles.geojson."),
    resampling_method: str = Option(..., help="Resampling method to use."),
    target_zoom: int = Option(..., help="Target zoom level."),
    target_prefix: str = Option(..., help="Destination S3 prefix."),
):

    log_queue = multiprocessing.Manager().Queue()
    listener = multiprocessing.Process(
        target=log_listener, args=(log_queue, listener_configurer)
    )
    listener.start()

    log_client_configurer(log_queue)
    logger = logging.getLogger("main")

    logger.log(logging.INFO, f"Reprojecting/resampling tiles in {source_uri}")

    src_tiles_info = get_source_tiles_info(source_uri)

    if not src_tiles_info:
        logger.log(logging.INFO, "No input files! I guess we're good then.")
        return

    base_dir = os.path.curdir

    source_dir = os.path.join(base_dir, "source_tiles")
    os.makedirs(source_dir, exist_ok=True)

    # First download all the source tiles
    dl_process_args = (
        (tile_info[0], source_dir, log_queue, log_client_configurer)
        for tile_info in src_tiles_info
    )

    # Cannot use normal pool here since we run sub-processes
    # https://stackoverflow.com/a/61470465/1410317
    tile_paths: List[str] = list()
    with ProcessPoolExecutor(max_workers=NUM_DL_PROCS) as executor:
        for tile_path in executor.map(download_tile, dl_process_args):
            tile_paths.append(tile_path)
            logger.log(logging.INFO, f"Finished downloading source tile to {tile_path}")

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
    logger.log(logging.INFO, "VRTs created")

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
            logger.log(logging.INFO, f"Tile {tile_id} intersects!")
            target_tiles.append((tile_id, (left, bottom, right, top)))

    logger.log(logging.INFO, f"Found {len(target_tiles)} intersecting tiles")

    bucket, _ = get_s3_path_parts(source_uri)

    process_tile_args = [
        (
            tile_id,
            tile_bounds,
            resampling_method,
            target_zoom,
            overall_vrt,
            bucket,
            target_prefix,
            log_queue,
            log_client_configurer,
        )
        for tile_id, tile_bounds in target_tiles
    ]

    # Cannot use normal pool here since we run sub-processes
    # https://stackoverflow.com/a/61470465/1410317
    with ProcessPoolExecutor(max_workers=NUM_TRANSFORM_PROCS) as executor:
        for tile_id in executor.map(process_tile, process_tile_args):
            logger.log(logging.INFO, f"Finished processing tile {tile_id}")

    # Now run pixetl_prep.create_geojsons to generate a tiles.geojson and
    # extent.geojson in the target prefix.
    # FIXME: Just does /geotiff. Do the same for the /gdal-geotiff folder too
    create_geojsons_prefix = target_prefix.split(f"{dataset}/{version}/")[1]
    logger.log(logging.INFO, f"Uploading tiles.geojson to {create_geojsons_prefix}")
    create_geojsons(list(), dataset, version, create_geojsons_prefix, True)

    log_queue.put_nowait(None)
    listener.join()


if __name__ == "__main__":
    try:
        run(resample)
    except (BrokenProcessPool, SubprocessKilledError):
        print("One of our subprocesses was killed! Exiting with 137")
        sys.exit(137)
