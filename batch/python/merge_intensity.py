#!/usr/bin/env python

import json
import os
import subprocess
from multiprocessing import Pool, cpu_count
from tempfile import TemporaryDirectory

import boto3
import click

from .logger import get_logger

AWS_REGION = os.environ.get("AWS_REGION")
AWS_ENDPOINT_URL = os.environ.get("ENDPOINT_URL")  # For boto
CORES = int(os.environ.get("CORES", cpu_count()))

logger = get_logger(__name__)


def get_s3_client(aws_region=AWS_REGION, endpoint_url=AWS_ENDPOINT_URL):
    return boto3.client("s3", region_name=aws_region, endpoint_url=endpoint_url)


def get_s3_path_parts(s3url):
    just_path = s3url.split("s3://")[1]
    bucket = just_path.split("/")[0]
    key = "/".join(just_path.split("/")[1:])
    return bucket, key


def get_tile_ids(bucket, key):
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=bucket, Key=key)
    geojson: dict = json.loads(response["Body"].read().decode("utf-8"))
    tiles = [
        os.path.basename(feature["properties"]["name"])
        for feature in geojson["features"]
    ]
    return tiles


@click.command()
@click.argument("date_conf_uri", type=str)
@click.argument("intensity_uri", type=str)
@click.argument("destination_prefix", type=str)
def merge_intensity(date_conf_uri, intensity_uri, destination_prefix):
    logger.info(f"Date/Confirmation status URI: {date_conf_uri}")
    logger.info(f"Intensity URI: {intensity_uri}")
    logger.info(f"Destination prefix: {destination_prefix}")

    bucket, date_conf_key = get_s3_path_parts(date_conf_uri)
    _, intensity_key = get_s3_path_parts(intensity_uri)

    # Get common tiles
    date_conf_tile_ids = get_tile_ids(bucket, date_conf_key)
    intensity_tile_ids = get_tile_ids(bucket, intensity_key)
    common_tile_ids = set(date_conf_tile_ids) & set(intensity_tile_ids)

    # Recreating full path

    date_conf_tiles = [
        os.path.join(os.path.dirname(date_conf_uri), tile_id)
        for tile_id in common_tile_ids
    ]
    intensity_tiles = [
        os.path.join(os.path.dirname(intensity_uri), tile_id)
        for tile_id in common_tile_ids
    ]
    output_tiles = [
        os.path.join(destination_prefix, tile_id) for tile_id in common_tile_ids
    ]
    tiles = zip(date_conf_tiles, intensity_tiles, output_tiles)

    # Process in parallel
    with Pool(processes=CORES) as pool:
        pool.starmap(process_rasters, tiles)


def process_rasters(date_conf_uri, intensity_uri, output_uri):
    s3_client = get_s3_client()

    # Download both files into a temporary directory
    with TemporaryDirectory() as temp_dir:
        local_date_conf_path = os.path.join(temp_dir, "date_conf.tif")
        local_intensity_path = os.path.join(temp_dir, "intensity.tif")
        local_output_path = os.path.join(temp_dir, "output.tif")

        logger.info(f"Downloading {date_conf_uri} to {local_date_conf_path}")
        bucket, key = get_s3_path_parts(date_conf_uri)
        s3_client.download_file(bucket, key, local_date_conf_path)

        logger.info(f"Downloading {intensity_uri} to {local_intensity_path}")
        bucket, key = get_s3_path_parts(intensity_uri)
        s3_client.download_file(bucket, key, local_intensity_path)

        # Run through build_rgb.cpp
        cmd_arg_list = [
            "build_rgb",
            local_date_conf_path,
            local_intensity_path,
            local_output_path,
        ]
        logger.info(f"Running command: {cmd_arg_list}")
        proc: subprocess.CompletedProcess = subprocess.run(
            cmd_arg_list, capture_output=True, check=False, text=True
        )
        logger.info(proc.stdout)
        logger.error(proc.stderr)
        proc.check_returncode()

        # Upload resulting output file to S3
        bucket, key = get_s3_path_parts(output_uri)

        logger.info(f"Uploading {local_output_path} to {output_uri}...")
        s3_client.upload_file(local_output_path, bucket, key)
    return


if __name__ == "__main__":
    exit(merge_intensity())
