#!/usr/bin/env python

import json
import os
import subprocess
from tempfile import TemporaryDirectory
from typing import Dict

import boto3
import click

# from fastapi.encoders import jsonable_encoder


AWS_REGION = "us-east-1"


def get_s3_client(aws_region=AWS_REGION, endpoint_url=os.environ["AWS_S3_ENDPOINT"]):
    return boto3.client("s3", region_name=aws_region, endpoint_url=endpoint_url)


def get_s3_path_parts(s3url):
    just_path = s3url.split("s3://")[1]
    bucket = just_path.split("/")[0]
    key = "/".join(just_path.split("/")[1:])
    return bucket, key


# def process_rasters(tile_set_asset_uri, destination_uri):
#     s3_client = get_s3_client()
#
#     # Download both files into a temporary directory
#     with TemporaryDirectory() as temp_dir:
#         local_date_conf_path = os.path.join(temp_dir, "date_conf.tiff")
#         local_intensity_path = os.path.join(temp_dir, "intensity.tiff")
#         local_output_path = os.path.join(temp_dir, "output.tiff")
#
#         print(f"Downloading {date_conf_uri} to {local_date_conf_path}")
#         bucket, key = get_s3_path_parts(date_conf_uri)
#         s3_client.download_file(bucket, key, local_date_conf_path)
#
#         print(f"Downloading {intensity_uri} to {local_intensity_path}")
#         bucket, key = get_s3_path_parts(intensity_uri)
#         s3_client.download_file(bucket, key, local_intensity_path)
#
#         # Run through build_rgb.cpp
#         cmd_arg_list = [
#             "build_rgb",
#             local_date_conf_path,
#             local_intensity_path,
#             local_output_path,
#         ]
#         print(f"Running command: {cmd_arg_list}")
#         _: subprocess.CompletedProcess = subprocess.run(
#             cmd_arg_list, capture_output=True, check=True, text=True
#         )
#
#         # FIXME: Do some checking for errors and whatnot
#
#         # Upload resulting output file to S3
#         bucket, key = get_s3_path_parts(output_uri)
#
#         print(f"Uploading {local_output_path} to {output_uri}...")
#         s3_client.upload_file(local_output_path, bucket, key)
#     return


def output_file_name(coordinates: str):
    # Construct an output file name based on a tile's coordinates
    pass


@click.command()
@click.option(
    "-d", "--dataset", type=str, required=True, help="Name of dataset to process"
)
@click.option(
    "-v", "--version", type=str, required=True, help="Version of dataset to process"
)
@click.argument("tile_set_asset_uri", type=str)
@click.argument("destination_uri", type=str)
def hello(dataset, version, tile_set_asset_uri, destination_uri):
    print(f"Raster tile set asset URI: {tile_set_asset_uri}")
    print(f"Destination URI: {destination_uri}")

    s3_client = get_s3_client()

    bucket, key = get_s3_path_parts(tile_set_asset_uri)
    prefix = key.replace("{tile_id}.tif", "")
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    # print(json.dumps(jsonable_encoder(resp), indent=2))
    if resp["KeyCount"] > 0:
        for obj in resp["Contents"]:
            remote_key = str(obj["Key"])
            if remote_key.endswith(".tif"):
                with TemporaryDirectory() as dl_dir:
                    local_tiff_path = os.path.join(dl_dir, "raster.tif")
                    remote_tiff_path = remote_key  # "/".join([prefix, remote_key])
                    print(f"Downloading {remote_tiff_path} to {local_tiff_path}")
                    s3_client.download_file(bucket, remote_tiff_path, local_tiff_path)

                    print(
                        f"Finished downloading {remote_tiff_path} to {local_tiff_path}"
                    )

                    with TemporaryDirectory() as tiles_dir:
                        cmd_arg_list = [
                            "gdal2tiles.py",
                            "--zoom=0-1",
                            "--s_srs",
                            "EPSG:3857",
                            "--resampling=near",
                            "--processes=1",
                            "--xyz",
                            local_tiff_path,
                            tiles_dir,
                        ]
                        print(f"Running command: {cmd_arg_list}")
                        proc: subprocess.CompletedProcess = subprocess.run(
                            cmd_arg_list,
                            capture_output=True,
                            check=False,
                            text=True
                            # cmd_arg_list, capture_output=True, check=True, text=True
                        )
                        print(proc.stdout)
                        print(proc.stderr)

                        # cmd_arg_list = [
                        #     "ls",
                        #     "/".join([tiles_dir, "0"]),
                        # ]
                        # proc: subprocess.CompletedProcess = subprocess.run(
                        #     cmd_arg_list, capture_output=True, check=False, text=True
                        #     # cmd_arg_list, capture_output=True, check=True, text=True
                        # )
                        # print(proc.stdout)

                        cmd_arg_list = [
                            "ls",
                            "-al",
                            tiles_dir,
                        ]
                        proc: subprocess.CompletedProcess = subprocess.run(
                            cmd_arg_list,
                            capture_output=True,
                            check=False,
                            text=True
                            # cmd_arg_list, capture_output=True, check=True, text=True
                        )
                        print(proc.stdout)

                        # FIXME: Do some checking for errors and whatnot

                        # # Upload resulting output file to S3
                        # bucket, key = get_s3_path_parts(output_uri)
                        #
                        # print(f"Uploading {local_output_path} to {output_uri}...")
                        # s3_client.upload_file(local_output_path, bucket, key)


if __name__ == "__main__":
    hello()
    exit(1)
