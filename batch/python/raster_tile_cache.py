#!/usr/bin/env python
import math
import multiprocessing
import os
import subprocess as sp
from concurrent.futures import ProcessPoolExecutor
from tempfile import TemporaryDirectory
from typing import Dict, List, Optional, Tuple

import boto3
from logger import get_logger
from tileputty.upload_tiles import upload_tiles
from typer import Argument, Option, run

AWS_REGION = os.environ.get("AWS_REGION")
AWS_ENDPOINT_URL = os.environ.get("ENDPOINT_URL")  # For boto
CORES = int(os.environ.get("CORES", multiprocessing.cpu_count()))

LOGGER = get_logger(__name__)


class GDALError(Exception):
    pass


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

    if p.returncode != 0:
        raise GDALError(e)

    return o, e


def get_input_tiles(prefix: str) -> List[Tuple[str, str]]:
    s3_client = get_s3_client()

    bucket, prefix = get_s3_path_parts(prefix)
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if resp["KeyCount"] == 0:
        raise Exception(f"No files found in tile set prefix {prefix}")

    tiles: List[Tuple[str, str]] = []
    for obj in resp["Contents"]:
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
        cores,
    ) = args

    with TemporaryDirectory() as download_dir, TemporaryDirectory() as tiles_dir:
        tile_name = os.path.join(download_dir, os.path.basename(tile[1]))
        s3_client = get_s3_client()
        s3_client.download_file(tile[0], tile[1], tile_name)

        cmd = [
            "16bpp_gdal2tiles.py",
            f"--zoom={zoom_level}",
            "--s_srs",
            "EPSG:3857",
            "--resampling=near",
            f"--processes={cores}",
            "--xyz",
        ]

        if skip_empty_tiles:
            cmd += ["-x"]

        cmd += [tile_name, tiles_dir]

        run_gdal_subcommand(cmd)

        LOGGER.info("Uploading tiles using TilePutty")
        upload_tiles(
            tiles_dir,
            dataset,
            version,
            cores=cores,
            bucket=target_bucket,
            implementation=implementation,
        )

    return tile


def raster_tile_cache(
    dataset: str = Option(..., help="Dataset name."),
    version: str = Option(..., help="Version number."),
    zoom_level: int = Option(..., help="Zoom level."),
    implementation: str = Option(..., help="Implementation name/ pixel meaning."),
    target_bucket: str = Option(..., help="Target bucket,"),
    skip_empty_tiles: bool = Option(
        False, "--skip_empty_tiles", help="Do not write empty tiles to tile cache."
    ),
    tile_set_prefix: str = Argument(..., help="Tile prefix,"),
):
    LOGGER.info(f"Raster tile set asset prefix: {tile_set_prefix}")

    tiles = get_input_tiles(tile_set_prefix)

    # If there are no files, what can we do? Just exit I guess!
    if not tiles:
        LOGGER.info("No input files! I guess we're good then?")
        return

    sub_processes = max(math.floor(CORES / len(tiles)), 1)

    args = [
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
    ]

    # Cannot use normal pool here, since we run sub-processes
    # https://stackoverflow.com/a/61470465/1410317
    with ProcessPoolExecutor(max_workers=CORES) as executor:
        for tile in executor.map(create_tiles, args):
            print(f"Processed tile {os.path.basename(tile[1])}")


if __name__ == "__main__":
    run(raster_tile_cache)
