#!/usr/bin/env python

import json
import os
import subprocess
from tempfile import TemporaryDirectory
from typing import Dict

import boto3
import click

AWS_REGION = os.environ.get("AWS_REGION")
AWS_ENDPOINT_URL = os.environ.get("ENDPOINT_URL")  # For boto


def get_s3_client(aws_region=AWS_REGION, endpoint_url=AWS_ENDPOINT_URL):
    return boto3.client("s3", region_name=aws_region, endpoint_url=endpoint_url)


def get_s3_path_parts(s3url):
    just_path = s3url.split("s3://")[1]
    bucket = just_path.split("/")[0]
    key = "/".join(just_path.split("/")[1:])
    return bucket, key


@click.command()
@click.argument("date_conf_uri", type=str)
@click.argument("intensity_uri", type=str)
@click.argument("destination_prefix", type=str)
def merge_intensity(date_conf_uri, intensity_uri, destination_prefix):
    print(f"Date/Confirmation status URI: {date_conf_uri}")
    print(f"Intensity URI: {intensity_uri}")
    print(f"Destination prefix: {destination_prefix}")

    s3_client = get_s3_client()

    geo_to_filenames: Dict[str, Dict[str, str]] = dict()

    # Get the tiles.geojsons and map coordinates to geoTIFFs
    d_c_f_n = "date_conf_file_name"
    i_f_n = "intensity_file_name"
    for input_pair in ((d_c_f_n, date_conf_uri), (i_f_n, intensity_uri)):
        bucket, key = get_s3_path_parts(input_pair[1])
        response = s3_client.get_object(Bucket=bucket, Key=key)
        tiles_geojson: dict = json.loads(response["Body"].read().decode("utf-8"))
        # print(f"TILES.GEOJSON: {json.dumps(tiles_geojson, indent=2)}")

        for feature in tiles_geojson["features"]:
            serialized_coords: str = json.dumps(feature["geometry"]["coordinates"])
            file_name = feature["properties"]["name"].replace("/vsis3/", "s3://")
            file_names_dict = geo_to_filenames.get(serialized_coords, {})
            if not file_names_dict:
                file_names_dict[input_pair[0]] = file_name
                geo_to_filenames[serialized_coords] = file_names_dict
            else:
                geo_to_filenames[serialized_coords][input_pair[0]] = file_name

    print(f"GEO_TO_FILENAMES: {json.dumps(geo_to_filenames, indent=2)}")

    # Only use coordinates that have associated files in both date/conf and
    # intensity tile sets.
    for k, v in geo_to_filenames.items():
        date_conf_uri = v.get(d_c_f_n)
        intensity_uri = v.get(i_f_n)
        if date_conf_uri is None:
            print(f"No date/conf raster file for coordinates {k}... Skipping")
            continue
        elif intensity_uri is None:
            print(f"No intensity raster file for coordinates {k}... Skipping")
            continue
        else:
            output_uri = "/".join([destination_prefix, intensity_uri.rsplit("/", 1)[1]])
            print("About to operate on:")
            print(f"date_conf file: {date_conf_uri}")
            print(f"intensity file: {intensity_uri}")
            print(f"output file: {output_uri}")

            process_rasters(date_conf_uri, intensity_uri, output_uri)


def process_rasters(date_conf_uri, intensity_uri, output_uri):
    s3_client = get_s3_client()

    # Download both files into a temporary directory
    with TemporaryDirectory() as temp_dir:
        local_date_conf_path = os.path.join(temp_dir, "date_conf.tiff")
        local_intensity_path = os.path.join(temp_dir, "intensity.tiff")
        local_output_path = os.path.join(temp_dir, "output.tiff")

        print(f"Downloading {date_conf_uri} to {local_date_conf_path}")
        bucket, key = get_s3_path_parts(date_conf_uri)
        s3_client.download_file(bucket, key, local_date_conf_path)

        print(f"Downloading {intensity_uri} to {local_intensity_path}")
        bucket, key = get_s3_path_parts(intensity_uri)
        s3_client.download_file(bucket, key, local_intensity_path)

        # Run through build_rgb.cpp
        cmd_arg_list = [
            "build_rgb",
            local_date_conf_path,
            local_intensity_path,
            local_output_path,
        ]
        print(f"Running command: {cmd_arg_list}")
        proc: subprocess.CompletedProcess = subprocess.run(
            cmd_arg_list, capture_output=True, check=False, text=True
        )
        print(proc.stdout)
        print(proc.stderr)
        proc.check_returncode()

        # Upload resulting output file to S3
        bucket, key = get_s3_path_parts(output_uri)

        print(f"Uploading {local_output_path} to {output_uri}...")
        s3_client.upload_file(local_output_path, bucket, key)
    return


if __name__ == "__main__":
    exit(merge_intensity())
