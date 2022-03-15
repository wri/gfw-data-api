#!/usr/bin/env python

import math
import multiprocessing
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from tempfile import TemporaryDirectory
from typing import List, Tuple

# Use relative imports because these modules get copied into container
from aws_utils import get_s3_client, get_s3_path_parts
from errors import SubprocessKilledError
from gdal_utils import run_gdal_subcommand
from logger import get_logger
from tileputty.upload_tiles import upload_tiles
from typer import Argument, Option, run

NUM_PROCESSES = int(
    os.environ.get(
        "NUM_PROCESSES", os.environ.get("CORES", multiprocessing.cpu_count())
    )
)
LOGGER = get_logger(__name__)


def get_input_tiles(prefix: str) -> List[Tuple[str, str]]:
    bucket, prefix = get_s3_path_parts(prefix)

    tiles: List[Tuple[str, str]] = list()

    s3_client = get_s3_client()

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        try:
            contents = page["Contents"]
        except KeyError:
            break

        for obj in contents:
            key = str(obj["Key"])
            if key.endswith(".tif"):
                LOGGER.info(f"Found remote TIFF: {key}")
                tile = (bucket, key)
                tiles.append(tile)

    return tiles


def create_tiles(args: Tuple[Tuple[str, str], str, str, str, str, int, bool, int, int]):
    (
        tile,
        dataset,
        version,
        target_bucket,
        implementation,
        zoom_level,
        skip_empty_tiles,
        sub_processes,
        bit_depth,
    ) = args

    with TemporaryDirectory() as download_dir, TemporaryDirectory() as tiles_dir:
        tile_name = os.path.basename(tile[1])
        tile_path = os.path.join(download_dir, tile_name)

        LOGGER.info(f"Beginning download of {tile_name}")
        get_s3_client().download_file(tile[0], tile[1], tile_path)

        if bit_depth == 8:
            gdal2tiles: str = "8bpp_gdal2tiles.py"
        else:
            gdal2tiles = "16bpp_gdal2tiles.py"

        cmd: List[str] = [
            gdal2tiles,
            f"--zoom={zoom_level}",
            "--s_srs",
            "EPSG:3857",
            "--resampling=near",
            f"--processes={sub_processes}",
            "--xyz",
        ]

        if skip_empty_tiles:
            cmd += ["-x"]

        cmd += [tile_path, tiles_dir]

        LOGGER.info(f"Running gdal2tiles on {tile_name}")
        run_gdal_subcommand(cmd)

        LOGGER.info(f"Uploading tiles for {tile_name} using TilePutty")
        upload_tiles(
            tiles_dir,
            dataset,
            version,
            cores=sub_processes,
            bucket=target_bucket,
            implementation=implementation,
        )

    return tile_name


def raster_tile_cache(
    dataset: str = Option(..., help="Dataset name."),
    version: str = Option(..., help="Version number."),
    zoom_level: int = Option(..., help="Zoom level."),
    implementation: str = Option(..., help="Implementation name/pixel meaning."),
    target_bucket: str = Option(..., help="Target bucket."),
    skip_empty_tiles: bool = Option(
        False, "--skip_empty_tiles", help="Do not write empty tiles to tile cache."
    ),
    bit_depth: int = Option(8, help="Number of bits per channel to use."),
    tile_set_prefix: str = Argument(..., help="Tile prefix."),
):
    LOGGER.info(f"Raster tile set asset prefix: {tile_set_prefix}")

    tiles: List[Tuple[str, str]] = get_input_tiles(tile_set_prefix)

    # If there are no files, what can we do? Just exit I guess!
    if not tiles:
        LOGGER.info("No input files! I guess we're good then?")
        return

    sub_processes = max(math.floor(NUM_PROCESSES / len(tiles)), 1)
    LOGGER.info(f"Using {sub_processes} sub-processes per process")

    args = (
        (
            tile,
            dataset,
            version,
            target_bucket,
            implementation,
            zoom_level,
            skip_empty_tiles,
            sub_processes,
            bit_depth,
        )
        for tile in tiles
    )

    # Cannot use normal pool here, since we run sub-processes
    # https://stackoverflow.com/a/61470465/1410317
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        for tile in executor.map(create_tiles, args):
            LOGGER.info(f"Finished processing tile {tile}")


if __name__ == "__main__":
    try:
        run(raster_tile_cache)
    except (BrokenProcessPool, SubprocessKilledError):
        LOGGER.error("One of our subprocesses was killed! Exiting with 137")
        sys.exit(137)
