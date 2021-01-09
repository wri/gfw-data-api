#!/usr/bin/env python
import os
import subprocess
from logging import getLogger
from tempfile import TemporaryDirectory

import boto3
import click
from tileputty.upload_tiles import upload_tiles

AWS_REGION = os.environ.get("AWS_REGION")
AWS_ENDPOINT_URL = os.environ.get("ENDPOINT_URL")  # For boto

logger = getLogger("generate_raster_tile_cache")


def get_s3_client(aws_region=AWS_REGION, endpoint_url=AWS_ENDPOINT_URL):
    return boto3.client("s3", region_name=aws_region, endpoint_url=endpoint_url)


def get_s3_path_parts(s3url):
    just_path = s3url.split("s3://")[1]
    bucket = just_path.split("/")[0]
    key = "/".join(just_path.split("/")[1:])
    return bucket, key


@click.command()
@click.option(
    "-d", "--dataset", type=str, required=True, help="Name of dataset to process"
)
@click.option(
    "-v", "--version", type=str, required=True, help="Version of dataset to process"
)
@click.option(
    "-I", "--implementation", type=str, required=True, help="Namespace for tile cache"
)
@click.option(
    "--target_bucket", type=str, required=True, help="S3 Bucket to upload tile cache to"
)
@click.option(
    "--zoom_level", type=int, required=True, help="Zoom level to generate tiles for"
)
@click.argument("tile_set_prefix", type=str)
def raster_tile_cache(
    dataset, version, zoom_level, implementation, target_bucket, tile_set_prefix
):
    logger.info(f"Raster tile set asset prefix: {tile_set_prefix}")

    s3_client = get_s3_client()

    bucket, prefix = get_s3_path_parts(tile_set_prefix)
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if resp["KeyCount"] == 0:
        raise Exception(f"No files found in tile set prefix {tile_set_prefix}")

    tiff_paths = []
    for obj in resp["Contents"]:
        remote_key = str(obj["Key"])
        if remote_key.endswith(".tif"):  # FIXME: Add case insensitivity?
            logger.info(f"Found remote TIFF: {remote_key}")
            gdal_path = os.path.join("/vsis3", bucket, remote_key)
            tiff_paths.append(gdal_path)

    # If there are no files, what can we do? Just exit I guess!
    if not tiff_paths:
        logger.info("No input files! I guess we're good then?")
        return

    with TemporaryDirectory() as vrt_dir, TemporaryDirectory() as tiles_dir:
        vrt_path = os.path.join(vrt_dir, "index.vrt")

        cmd_arg_list = ["gdalbuildvrt", vrt_path, *tiff_paths]
        logger.info(f"Running command: {cmd_arg_list}")
        proc: subprocess.CompletedProcess = subprocess.run(
            cmd_arg_list, capture_output=True, check=False, text=True
        )
        logger.info(proc.stdout)
        logger.error(proc.stderr)
        proc.check_returncode()

        cmd_arg_list = [
            "gdal2tiles.py",
            f"--zoom={zoom_level}",
            "--s_srs",
            "EPSG:3857",
            "--resampling=near",
            f"--processes={os.environ.get('CORES', os.cpu_count())}",
            "--xyz",
            vrt_path,
            tiles_dir,
        ]
        logger.info(f"Running command: {cmd_arg_list}")
        proc: subprocess.CompletedProcess = subprocess.run(
            cmd_arg_list,
            capture_output=True,
            check=False,
            text=True,
        )
        logger.info(proc.stdout)
        logger.error(proc.stderr)
        proc.check_returncode()

        logger.info("Uploading tiles using TilePutty")
        upload_tiles(
            tiles_dir,
            dataset,
            version,
            bucket=target_bucket,
            implementation=implementation,
        )


if __name__ == "__main__":
    raster_tile_cache()
