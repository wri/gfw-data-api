#!/usr/bin/env python
import copy
import json
import multiprocessing
import os
import subprocess as sp
from concurrent.futures import ProcessPoolExecutor
from enum import Enum
from logging import getLogger
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
from gfw_pixetl.pixetl_prep import create_geojsons
from pydantic import BaseModel, Extra, Field, StrictInt
from typer import Option, run

AWS_REGION = os.environ.get("AWS_REGION")
AWS_ENDPOINT_URL = os.environ.get("ENDPOINT_URL")  # For boto
CORES = int(os.environ.get("CORES", multiprocessing.cpu_count()))

OrderedColorMap = Dict[Union[int, float], Tuple[int, int, int, int]]

GEOTIFF_COMPRESSION = "DEFLATE"
GDAL_GEOTIFF_COMPRESSION = "DEFLATE"

LOGGER = getLogger("apply_symbology")


class ColorMapType(str, Enum):
    discrete = "discrete"
    gradient = "gradient"


class GDALError(Exception):
    pass


class StrictBaseModel(BaseModel):
    class Config:
        extra = Extra.forbid
        validate_assignment = True


class RGBA(StrictBaseModel):
    red: int = Field(..., ge=0, le=255)
    green: int = Field(..., ge=0, le=255)
    blue: int = Field(..., ge=0, le=255)
    alpha: int = Field(255, ge=0, le=255)

    def tuple(self) -> Tuple[int, int, int, int]:
        return self.red, self.green, self.blue, self.alpha


class Symbology(StrictBaseModel):
    type: ColorMapType
    colormap: Optional[Dict[Union[StrictInt, float], RGBA]]


def get_s3_client(aws_region=AWS_REGION, endpoint_url=AWS_ENDPOINT_URL):
    return boto3.client("s3", region_name=aws_region, endpoint_url=endpoint_url)


def get_s3_path_parts(s3url) -> Tuple[str, str]:
    just_path = s3url.split("s3://")[1]
    bucket = just_path.split("/")[0]
    key = "/".join(just_path.split("/")[1:])
    return bucket, key


def from_vsi(file_name: str) -> str:
    """Convert /vsi path to s3 or gs path.

    Stolen from pixetl
    """

    protocols = {"vsis3": "s3", "vsigs": "gs"}

    parts = file_name.split("/")
    try:
        vsi = f"{protocols[parts[1]]}://{'/'.join(parts[2:])}"
    except KeyError:
        raise ValueError(f"Unknown protocol: {parts[1]}")
    return vsi


def get_source_tile_uris(tiles_geojson_uri):
    s3_client = get_s3_client()
    bucket, key = get_s3_path_parts(tiles_geojson_uri)

    response = s3_client.get_object(Bucket=bucket, Key=key)
    tiles_geojson: Dict[str, Any] = json.loads(response["Body"].read().decode("utf-8"))
    tiles = [
        from_vsi(feature["properties"]["name"]) for feature in tiles_geojson["features"]
    ]
    return tiles


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
        LOGGER.error(f"Exit code {p.returncode} for command {cmd}")
        LOGGER.error(f"Standard output: {o}")
        LOGGER.error(f"Standard error: {e}")
        raise GDALError(e)

    return o, e


def _sort_colormap(
    no_data_value: Optional[Union[StrictInt, float]], symbology: Symbology
) -> OrderedColorMap:
    """
    Create value - quadruplet colormap (GDAL format) including no data value.

    """
    assert symbology.colormap, "No colormap specified."

    colormap: Dict[Union[StrictInt, float], RGBA] = copy.deepcopy(symbology.colormap)

    ordered_gdal_colormap: OrderedColorMap = dict()

    # add no data value to colormap, if exists
    # (not sure why mypy throws an error here, hence type: ignore)
    if no_data_value is not None:
        colormap[no_data_value] = RGBA(red=0, green=0, blue=0, alpha=0)  # type: ignore

    # make sure values are correctly sorted and convert to value-quadruplet string
    for pixel_value in sorted(colormap.keys()):
        ordered_gdal_colormap[pixel_value] = colormap[pixel_value].tuple()

    return ordered_gdal_colormap


def create_rgb_tile(args: Tuple[str, str, ColorMapType, str]) -> str:
    """Add symbology to output raster.

    Gradient colormap: Use linear interpolation based on provided
    colormap to compute RGBA quadruplet for any given pixel value.
    Discrete colormap: Use strict matching when searching in the color
    configuration file. If no matching color entry is found, the
    “0,0,0,0” RGBA quadruplet will be used.
    """
    source_tile_uri, target_prefix, symbology_type, colormap_path = args

    tile_id = os.path.splitext(os.path.basename(source_tile_uri))[0]

    with TemporaryDirectory() as work_dir:
        local_src_file_path = os.path.join(work_dir, os.path.basename(source_tile_uri))
        s3_client = get_s3_client()
        bucket, source_key = get_s3_path_parts(source_tile_uri)
        s3_client.download_file(bucket, source_key, local_src_file_path)

        local_dest_file_path = os.path.join(work_dir, f"{tile_id}_colored.tif")

        block_size = 256  # Block size for WebMercator Tiles is always 256x256

        cmd = [
            "gdaldem",
            "color-relief",
            "-alpha",
            "-co",
            f"COMPRESS={GEOTIFF_COMPRESSION}",
            "-co",
            "TILED=YES",
            "-co",
            f"BLOCKXSIZE={block_size}",
            "-co",
            f"BLOCKYSIZE={block_size}",
            "-co",
            "SPARSE_OK=TRUE",
            "-co",
            "INTERLEAVE=BAND",
        ]

        if symbology_type == ColorMapType.discrete:
            cmd += ["-exact_color_entry"]

        cmd += [local_src_file_path, colormap_path, local_dest_file_path]

        try:
            run_gdal_subcommand(cmd)
        except GDALError:
            LOGGER.error(f"Could not create Color Relief for tile_id {tile_id}")
            raise

        # Now upload the file to S3
        target_key = os.path.join(target_prefix, os.path.basename(local_src_file_path))
        s3_client.upload_file(local_dest_file_path, bucket, target_key)

    return tile_id


def apply_symbology(
    dataset: str = Option(..., help="Dataset name."),
    version: str = Option(..., help="Version string."),
    symbology: str = Option(..., help="Symbology JSON."),
    no_data: str = Option(..., help="JSON-encoded no data value."),
    source_uri: str = Option(..., help="URI of source tiles.geojson."),
    target_prefix: str = Option(..., help="Target prefix."),
):
    LOGGER.info(f"Applying symbology to tiles listed in {source_uri}")

    tile_uris = get_source_tile_uris(source_uri)

    if not tile_uris:
        LOGGER.info("No input files! I guess we're good then.")
        return

    no_data_value: Optional[Union[StrictInt, float]] = json.loads(no_data)

    symbology_obj = Symbology(**json.loads(symbology))
    ordered_colormap: OrderedColorMap = _sort_colormap(no_data_value, symbology_obj)

    # Write the colormap to a file in a temporary directory
    with TemporaryDirectory() as tmp_dir:
        colormap_path = os.path.join(tmp_dir, "colormap.txt")
        with open(colormap_path, "w") as colormap_file:
            for pixel_value in ordered_colormap:
                values = [str(pixel_value)] + [
                    str(i) for i in ordered_colormap[pixel_value]
                ]
                row = " ".join(values)
                colormap_file.write(row)
                colormap_file.write("\n")

        process_args = (
            (tile_uri, target_prefix, symbology_obj.type, colormap_path)
            for tile_uri in tile_uris
        )

        # Cannot use normal pool here since we run sub-processes
        # https://stackoverflow.com/a/61470465/1410317
        with ProcessPoolExecutor(max_workers=CORES) as executor:
            for tile_id in executor.map(create_rgb_tile, process_args):
                print(f"Processed tile {tile_id}")

    # Now run pixetl_prep.create_geojsons to generate a tiles.geojson and
    # extent.geojson in the target prefix. But that code appends /geotiff
    # to the prefix so remove it first
    create_geojsons_prefix = target_prefix.split(f"{dataset}/{version}/")[1].replace(
        "/geotiff", ""
    )
    create_geojsons(list(), dataset, version, create_geojsons_prefix, True)


if __name__ == "__main__":
    run(apply_symbology)
