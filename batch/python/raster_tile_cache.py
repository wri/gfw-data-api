#!/usr/bin/env python

import math
import multiprocessing
import os
import subprocess as sp
import sys
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from tempfile import TemporaryDirectory
from typing import Dict, List, Optional, Tuple

import boto3
from errors import GDALError, SubprocessKilledError
from logger import get_logger
from tileputty.upload_tiles import upload_tiles
from typer import Argument, Option, run

AWS_REGION = os.environ.get("AWS_REGION")
AWS_ENDPOINT_URL = os.environ.get("ENDPOINT_URL")  # For boto
NUM_PROCESSES = int(
    os.environ.get(
        "NUM_PROCESSES", os.environ.get("CORES", multiprocessing.cpu_count())
    )
)
LOGGER = get_logger(__name__)


def get_s3_client(aws_region=AWS_REGION, endpoint_url=AWS_ENDPOINT_URL):
    return boto3.client("s3", region_name=aws_region, endpoint_url=endpoint_url)


def get_s3_path_parts(s3url):
    just_path = s3url.split("s3://")[1]
    bucket = just_path.split("/")[0]
    key = "/".join(just_path.split("/")[1:])
    return bucket, key


def run_gdal_subcommand(cmd: List[str], env: Optional[Dict] = None) -> Tuple[str, str]:
    """Run GDAL as sub command and catch common errors."""

    gdal_env = os.environ.copy()
    if env:
        gdal_env.update(**env)

    LOGGER.debug(f"RUN subcommand {cmd}, using env {gdal_env}")
    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, env=gdal_env)

    o_byte, e_byte = p.communicate()

    # somehow return type when running `gdalbuildvrt` is str but otherwise bytes
    try:
        o = o_byte.decode("utf-8")
        e = e_byte.decode("utf-8")
    except AttributeError:
        o = str(o_byte)
        e = str(e_byte)

    if p.returncode < 0:
        raise SubprocessKilledError()
    elif p.returncode != 0:
        raise GDALError(e)

    return o, e


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


def create_tiles(args: Tuple[Tuple[str, str], str, str, str, str, int, bool, int]):
    (
        tile,
        dataset,
        version,
        target_bucket,
        implementation,
        zoom_level,
        skip_empty_tiles,
        sub_processes,
    ) = args

    with TemporaryDirectory() as download_dir, TemporaryDirectory() as tiles_dir:
        tile_name = os.path.basename(tile[1])
        tile_path = os.path.join(download_dir, tile_name)

        LOGGER.info(f"Beginning download of {tile_name}")
        get_s3_client().download_file(tile[0], tile[1], tile_path)

        cmd = [
            # "16bpp_gdal2tiles.py",
            "gdal2tiles.py",
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
        LOGGER.error("One of our subprocesses as killed! Exiting with 137")
        sys.exit(137)
