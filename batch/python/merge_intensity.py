#!/usr/bin/env python

import json
import os
import subprocess
from tempfile import TemporaryDirectory
from typing import Dict

import boto3
import click

AWS_REGION = "us-east-1"


def get_s3_client(aws_region=AWS_REGION, endpoint_url=os.environ["AWS_S3_ENDPOINT"]):
    return boto3.client("s3", region_name=aws_region, endpoint_url=endpoint_url)


def get_s3_path_parts(s3url):
    just_path = s3url.split("s3://")[1]
    bucket = just_path.split("/")[0]
    key = "/".join(just_path.split("/")[1:])
    return bucket, key


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
        # FIXME: Just touch the output file for now
        subprocess.CompletedProcess = subprocess.run(
            ["touch", local_output_path], capture_output=True, check=True, text=True
        )
        # proc_obj: subprocess.CompletedProcess = subprocess.run(
        #     cmd_arg_list,
        #     capture_output=True,
        #     check=True,
        #     text=True
        # )
        # Do some checking for errors and whatnot

        # Upload resulting output file to... where?
        bucket, key = get_s3_path_parts(output_uri)

        print(f"Uploading {local_output_path} to {output_uri}...")
        s3_client.upload_file(local_output_path, bucket, key)
    return


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
@click.argument("date_conf_uri", type=str)
@click.argument("intensity_uri", type=str)
@click.argument("destination_uri", type=str)
def hello(dataset, version, date_conf_uri, intensity_uri, destination_uri):
    print(f"Date/Confirmation status URI: {date_conf_uri}")
    print(f"Intensity URI: {intensity_uri}")

    geo_to_filenames: Dict[str, Dict[str, str]] = dict()

    s3_client = get_s3_client()

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
            blah = geo_to_filenames.get(serialized_coords, {})
            file_name = feature["properties"]["name"].replace("/vsis3/", "s3://")
            if not blah:
                blah[input_pair[0]] = file_name
                geo_to_filenames[serialized_coords] = blah
            else:
                geo_to_filenames[serialized_coords][input_pair[0]] = file_name

    # Verify that all coordinates have associated files in both date/conf and
    # intensity tile sets
    # FIXME: Don't assert, just use those tiles with files in both datasets
    print(f"GEO_TO_FILENAMES: {json.dumps(geo_to_filenames, indent=2)}")
    for coordinate_set in geo_to_filenames.values():
        print(f"COORD_SET: {coordinate_set}")
        # assert coordinate_set.get(d_c_f_n) is not None
        # assert coordinate_set.get(i_f_n) is not None

    for k, v in geo_to_filenames.items():
        # FIXME: Generate output filename based on coordinates
        # FIXME: Where to get prefix? What to call it? Hard-code as "combined" for now
        output_uri = v[i_f_n].replace("intensity", "combined")
        print("About to operate on:")
        print(f"date_conf file: {v[d_c_f_n]}")
        print(f"intensity file: {v[i_f_n]}")
        print(f"outout file: {output_uri}")

        process_rasters(v[d_c_f_n], v[i_f_n], output_uri)
    # FIXME: Create extent.geojson and tiles.geojson for the combined tiles. Or do back in raster tile cache asset code?


if __name__ == "__main__":
    hello()
