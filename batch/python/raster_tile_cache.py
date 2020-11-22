#!/usr/bin/env python

import os
import subprocess
from tempfile import TemporaryDirectory

import boto3
import click


def get_s3_client(
    aws_region=os.environ["AWS_REGION"], endpoint_url=os.environ["AWS_S3_ENDPOINT"]
):
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
    # FIXME: Download all files, gdalbuildvrt VRT and process the VRT

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
                            "--zoom=0",  # FIXME: Specify actual zoom level
                            "--s_srs",
                            "EPSG:3857",
                            "--resampling=near",
                            "--processes=1",  # FIXME: Pass in pixetl cores value? Or similar
                            "--xyz",
                            local_tiff_path,
                            tiles_dir,
                        ]
                        print(f"Running command: {cmd_arg_list}")
                        proc: subprocess.CompletedProcess = subprocess.run(
                            cmd_arg_list, capture_output=True, check=False, text=True
                        )
                        print(proc.stdout)
                        print(proc.stderr)

                        cmd_arg_list = [
                            "ls",
                            "-al",
                            tiles_dir,
                        ]
                        proc: subprocess.CompletedProcess = subprocess.run(
                            cmd_arg_list, capture_output=True, check=False, text=True
                        )
                        print(proc.stdout)

                        # FIXME: Do some checking for errors and whatnot

                        # FIXME: Use tileputty to upload tile cache

                        # # Upload resulting output file to S3
                        # bucket, key = get_s3_path_parts(output_uri)
                        #
                        # print(f"Uploading {local_output_path} to {output_uri}...")
                        # s3_client.upload_file(local_output_path, bucket, key)


if __name__ == "__main__":
    hello()
