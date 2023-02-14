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
from multiprocessing.queues import Queue
from typing import Any, Callable, Dict, List, Optional, Tuple

import psutil
import rasterio

# Use relative imports because these modules get copied into container
from aws_utils import exists_in_s3, get_s3_client, get_s3_path_parts
from errors import SubprocessKilledError
from gdal_utils import from_vsi_path
from gfw_pixetl.grids import grid_factory
from gfw_pixetl.pixetl_prep import create_geojsons
from logging_utils import listener_configurer, log_client_configurer, log_listener
from pyproj import CRS, Transformer
from shapely.geometry import MultiPolygon, Polygon, shape
from shapely.ops import unary_union
from typer import Option, run

# Use at least 1 process
# Try to get NUM_PROCESSES, if that fails get # CPUs divided by 1.5
NUM_PROCESSES = max(
    1,
    int(os.environ.get("NUM_PROCESSES", multiprocessing.cpu_count() // 1.5)) // 2,
)

MEM_PER_PROC = (psutil.virtual_memory()[1] // 1000000) // NUM_PROCESSES

# Remember, GDAL interprets >10k as bytes instead of MB
WARP_MEM = min(4096, int(MEM_PER_PROC * 0.7))
CACHE_MEM = min(1024, int(MEM_PER_PROC * 0.08))

GEOTIFF_COMPRESSION = "DEFLATE"

# Tiles.geojson feature coords always seem to be expressed in lat/lng
TILES_GEOJSON_CRS = CRS.from_epsg(4326)
# This script always creates WM tiles
TARGET_CRS = CRS.from_epsg(3857)

GDAL_TRANSLATE_RESAMPLING_METHODS = (
    "nearest",
    "bilinear",
    "cubic",
    "cubicspline",
    "lanczos",
    "average",
    "rms",
    "mode",
)

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


def get_source_tiles_info(tiles_geojson_uri) -> List[Tuple[str, Any]]:
    """Returns a list of tuples, each of which is the URL of a file referenced
    in the target tiles.geojson along with its GeoJSON feature."""
    s3_client = get_s3_client()
    bucket, key = get_s3_path_parts(tiles_geojson_uri)

    response = s3_client.get_object(Bucket=bucket, Key=key)
    tiles_geojson: Dict[str, Any] = json.loads(response["Body"].read().decode("utf-8"))
    tiles_info: List[Tuple[str, Any]] = [
        (from_vsi_path(feature["properties"]["name"]), feature["geometry"])
        for feature in tiles_geojson["features"]
    ]
    return tiles_info


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


def scale_raster(
    tile_bounds: Bounds,
    width: int,
    height: int,
    resampling_method: str,
    vrt_path,
    scaled_tile_path,
    logger,
):
    """Scale/extract a raster tile from a VRT with gdal_translate."""
    cmd: List[str] = [
        "gdal_translate",
        "-r",
        resampling_method,
        "-projwin",
        f"{tile_bounds[0]}",
        f"{tile_bounds[3]}",
        f"{tile_bounds[2]}",
        f"{tile_bounds[1]}",
        "-outsize",
        f"{width}",
        f"{height}",
        "-co",
        f"COMPRESS={GEOTIFF_COMPRESSION}",
        "-co",
        "TILED=YES",
        vrt_path,
        scaled_tile_path,
    ]
    env = dict(
        os.environ,
        GDAL_CACHEMAX=f"{CACHE_MEM}",
        VRT_SHARED_SOURCE="0",
    )

    logger.log(
        logging.INFO,
        f"Begin scaling/extracting {vrt_path} to {scaled_tile_path} with the command {cmd}",
    )

    tic = time.perf_counter()
    proc = subprocess.Popen(
        cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    pid = proc.pid
    while proc.poll() is None:
        mem = psutil.Process(pid).memory_info().rss / (1024**2)
        logger.log(
            logging.INFO,
            f"Scaling process {pid} currently consuming {mem} MB of memory",
        )
        time.sleep(10)
    toc = time.perf_counter()

    if proc.returncode == -9:
        logger.log(
            logging.ERROR,
            f"Scaling subprocess {pid} killed with signal 9 (likely OOM)!",
        )
        raise SubprocessKilledError
    if proc.returncode != 0:
        logger.log(
            logging.ERROR,
            f"Scaling subprocess {pid} FAILED with exit code {proc.returncode}",
        )
        logger.log(logging.ERROR, proc.stdout)
        raise Exception(f"Scaling {vrt_path} failed")

    logger.log(
        logging.INFO,
        f"Scaling {vrt_path} to {scaled_tile_path} took {toc - tic:0.4f} seconds",
    )
    return toc - tic


def warp_raster(
    bounds: Bounds, width, height, resampling_method, source_path, target_path, logger
):
    """Warps/extracts raster of provided dimensions from source file."""
    cmd: List[str] = [
        "gdalwarp",
        "-t_srs",
        f"{TARGET_CRS}",
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
        "-wm",
        f"{WARP_MEM}",
        source_path,
        target_path,
    ]
    env = dict(
        os.environ,
        GDAL_CACHEMAX=f"{CACHE_MEM}",
        VRT_SHARED_SOURCE="0",
    )

    logger.log(
        logging.INFO,
        f"Begin warping of {source_path} to {target_path} with the command {cmd}",
    )

    tic = time.perf_counter()
    proc = subprocess.Popen(
        cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    pid = proc.pid
    while proc.poll() is None:
        mem = psutil.Process(pid).memory_info().rss / (1024**2)
        logger.log(
            logging.INFO, f"Warp process {pid} currently consuming {mem} MB of memory"
        )
        time.sleep(10)
    toc = time.perf_counter()

    if proc.returncode == -9:
        logger.log(
            logging.ERROR,
            "Warping subprocess killed with signal 9 (likely OOM)!",
        )
        raise SubprocessKilledError
    if proc.returncode != 0:
        logger.log(
            logging.ERROR,
            f"Warping FAILED with exit code {proc.returncode}",
        )
        logger.log(logging.ERROR, proc.stdout)
        raise Exception(f"Warping {source_path} failed")

    logger.log(
        logging.INFO,
        f"Warping {source_path} to {target_path} took {toc - tic:0.4f} seconds",
    )
    return toc - tic


def compress_raster(source_path, target_path, logger):
    """Compress a raster tile with gdal_translate."""
    cmd: List[str] = [
        "gdal_translate",
        "-co",
        f"COMPRESS={GEOTIFF_COMPRESSION}",
        "-co",
        "TILED=YES",
        source_path,
        target_path,
    ]
    env = dict(
        os.environ,
        GDAL_CACHEMAX=f"{CACHE_MEM}",
        VRT_SHARED_SOURCE="0",
    )

    logger.log(
        logging.INFO,
        f"Begin compression of {source_path} to {target_path} with the command {cmd}",
    )

    tic = time.perf_counter()
    proc = subprocess.Popen(
        cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    pid = proc.pid
    while proc.poll() is None:
        mem = psutil.Process(pid).memory_info().rss / (1024**2)
        logger.log(
            logging.INFO,
            f"Compression process {pid} currently consuming {mem} MB of memory",
        )
        time.sleep(10)
    toc = time.perf_counter()

    if proc.returncode == -9:
        logger.log(
            logging.ERROR,
            f"Compression subprocess {pid} killed with signal 9 (likely OOM)!",
        )
        raise SubprocessKilledError
    if proc.returncode != 0:
        logger.log(
            logging.ERROR,
            f"Compressing subprocess {pid} FAILED with exit code {proc.returncode}",
        )
        logger.log(logging.ERROR, proc.stdout)
        raise Exception(f"Compressing {source_path} failed")

    logger.log(
        logging.INFO,
        f"Compressing {source_path} to {target_path} took {toc - tic:0.4f} seconds",
    )
    return toc - tic


def process_tile(
    args: Tuple[str, Bounds, str, int, str, str, str, CRS, Queue, Callable]
) -> str:
    """Extract a tile from a source VRT, compress and upload it."""
    (
        tile_id,
        tile_bounds,
        resampling_method,
        target_zoom,
        vrt_path,
        target_bucket,
        target_prefix,
        source_crs,
        q,
        q_configurer,
    ) = args

    q_configurer(q)
    logger = logging.getLogger("process_tile")
    logger.log(logging.INFO, f"Begin processing tile {tile_id}")

    tile_file_name = f"{tile_id}.tif"
    target_geotiff_key = os.path.join(target_prefix, "geotiff", tile_file_name)

    if exists_in_s3(target_bucket, target_geotiff_key):
        logger.log(logging.INFO, f"Tile {tile_id} already exists in S3, skipping")
        return tile_id

    warp_dir = os.path.join(os.path.curdir, "warped")
    os.makedirs(warp_dir, exist_ok=True)
    warped_tile_path = os.path.join(warp_dir, tile_file_name)

    compressed_dir = os.path.join(os.path.curdir, "compressed")
    os.makedirs(compressed_dir, exist_ok=True)
    compressed_tile_path = os.path.join(compressed_dir, tile_file_name)

    tiles_in_zoom_level = max(1, int(2**target_zoom / 256)) ** 2
    width = height = int(2**target_zoom * 256 / math.sqrt(tiles_in_zoom_level))

    # For some situations (scaling within the same CRS) we can use just
    # gdal_translate instead of gdalwarp... and gdal_translate seems to be
    # much faster.
    if (
        source_crs == TARGET_CRS
        and resampling_method in GDAL_TRANSLATE_RESAMPLING_METHODS
    ):
        scale_raster(
            tile_bounds,
            width,
            height,
            resampling_method,
            vrt_path,
            compressed_tile_path,
            logger,
        )
    else:
        # If the compressed file exists, we know we at least started the
        # compression step, but don't know if it finished. So remove that file
        # and re-compress from the warped file (which should exist). If the
        # compressed file doesn't exist at all, run starting with the warp
        # command.
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
    if os.path.isfile(warped_tile_path):
        os.remove(warped_tile_path)

    return tile_id


def intersecting_tiles(
    source_crs: CRS,
    src_tiles_info: List[Tuple[str, Any]],
    target_grid_name: str,
    logger,
) -> List[Tuple[str, Bounds]]:
    """Find all tiles in the target zoom level which intersect the source
    tiles."""
    target_grid = grid_factory(f"{target_grid_name}")
    all_target_grid_tile_ids = target_grid.get_tile_ids()
    logger.log(
        logging.INFO,
        f"Target grid ({target_grid_name}) has {len(all_target_grid_tile_ids)} tiles",
    )

    extent_in_target_grid = MultiPolygon()
    for tile_info in src_tiles_info:
        left, bottom, right, top = reproject_bounds(
            shape(tile_info[1]).bounds, source_crs, TARGET_CRS
        )
        extent_in_target_grid = unary_union(
            [
                extent_in_target_grid,
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
    logger.log(
        logging.INFO, f"Source tiles extent in target grid: {extent_in_target_grid}"
    )

    tiles_in_target_grid: List[Tuple[str, Bounds]] = list()
    for tile_id in all_target_grid_tile_ids:
        left, bottom, right, top = target_grid.get_tile_bounds(tile_id)

        tile_geom = Polygon(
            ((left, top), (right, top), (right, bottom), (left, bottom), (left, top))
        )

        if tile_geom.intersects(extent_in_target_grid) and not tile_geom.touches(
            extent_in_target_grid
        ):
            logger.log(
                logging.INFO,
                f"Tile {tile_id} of grid {target_grid_name} intersects the source data",
            )
            tiles_in_target_grid.append((tile_id, (left, bottom, right, top)))

    logger.log(
        logging.INFO,
        f"Found {len(tiles_in_target_grid)} tiles in the target grid "
        f"which intersect the source tiles: {tiles_in_target_grid}",
    )
    return tiles_in_target_grid


def resample(
    dataset: str = Option(..., help="Dataset name."),
    version: str = Option(..., help="Version string."),
    source_uri: str = Option(..., help="URI of source asset's tiles.geojson."),
    resampling_method: str = Option(..., help="Resampling method to use."),
    target_zoom: int = Option(..., help="Target zoom level."),
    target_prefix: str = Option(..., help="Destination S3 prefix."),
):
    """Resample source raster tile set to target zoom level."""
    log_queue = multiprocessing.Manager().Queue()
    listener = multiprocessing.Process(
        target=log_listener, args=(log_queue, listener_configurer)
    )
    listener.start()

    log_client_configurer(log_queue)
    logger = logging.getLogger("main")

    logger.log(logging.INFO, f"Resampling tiles in {source_uri}")
    logger.log(
        logging.INFO,
        f"# procs:{NUM_PROCESSES} MEM_PER_PROC:{MEM_PER_PROC} WARP_MEM:{WARP_MEM} CACHE_MEM:{CACHE_MEM}",
    )

    src_tiles_info: List[Tuple[str, Any]] = get_source_tiles_info(source_uri)

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
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        for tile_path in executor.map(download_tile, dl_process_args):
            tile_paths.append(tile_path)
            logger.log(logging.INFO, f"Finished downloading source tile to {tile_path}")

    # Create VRTs for each of the bands of each of the input files
    # In this case we can assume each file has the same number of bands
    input_vrts: List[str] = list()
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

    # Determine which tiles in the target CRS + zoom level intersect with the
    # extent of the source

    # NOTE: pixetl seems to always write features in tiles.geojson in
    # epsg:4326 coordinates (even when the tiles themselves are
    # epsg:3857). If that ever changes, update TILES_GEOJSON_CRS
    # or get the CRS from tiles.geojson dynamically
    target_grid_name = f"zoom_{target_zoom}"
    tiles_in_target_grid: List[Tuple[str, Bounds]] = intersecting_tiles(
        TILES_GEOJSON_CRS, src_tiles_info, target_grid_name, logger
    )

    # Now get the ACTUAL CRS of the source tiles
    with rasterio.open(overall_vrt) as src_vrt:
        source_crs: CRS = src_vrt.crs

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
            source_crs,
            log_queue,
            log_client_configurer,
        )
        for tile_id, tile_bounds in tiles_in_target_grid
    ]

    # Cannot use normal pool here since we run sub-processes
    # https://stackoverflow.com/a/61470465/1410317
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        for tile_id in executor.map(process_tile, process_tile_args):
            logger.log(logging.INFO, f"Finished processing tile {tile_id}")

    # Now run pixetl_prep.create_geojsons to generate a tiles.geojson and
    # extent.geojson in the target prefix.
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
