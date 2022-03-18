#!/usr/bin/env python

import copy
import json
import logging
import multiprocessing
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from enum import Enum
from tempfile import TemporaryDirectory
from typing import Any, Dict, Optional, Tuple, Union

import rasterio

# Use relative imports because these modules get copied into container
from aws_utils import get_s3_client, get_s3_path_parts
from errors import GDALError, SubprocessKilledError
from gdal_utils import from_vsi_path, run_gdal_subcommand
from logging_utils import listener_configurer, log_client_configurer, log_listener
from pydantic import BaseModel, Extra, Field, StrictInt
from typer import Option, run

NUM_PROCESSES = int(
    os.environ.get(
        "NUM_PROCESSES", os.environ.get("CORES", multiprocessing.cpu_count())
    )
)
GEOTIFF_COMPRESSION = "DEFLATE"
GDAL_GEOTIFF_COMPRESSION = "DEFLATE"

OrderedColorMap = Dict[
    Union[int, float], Union[Tuple[int, int, int], Tuple[int, int, int, int]]
]


class ColorMapType(str, Enum):
    discrete = "discrete"
    discrete_intensity = "discrete_intensity"
    gradient = "gradient"
    gradient_intensity = "gradient_intensity"


class StrictBaseModel(BaseModel):
    class Config:
        extra = Extra.forbid
        validate_assignment = True


class RGB(StrictBaseModel):
    red: int = Field(..., ge=0, le=255)
    green: int = Field(..., ge=0, le=255)
    blue: int = Field(..., ge=0, le=255)

    def tuple(self) -> Tuple[int, int, int]:
        return self.red, self.green, self.blue


class RGBA(StrictBaseModel):
    red: int = Field(..., ge=0, le=255)
    green: int = Field(..., ge=0, le=255)
    blue: int = Field(..., ge=0, le=255)
    alpha: int = Field(..., ge=0, le=255)

    def tuple(self) -> Tuple[int, int, int, int]:
        return self.red, self.green, self.blue, self.alpha


class Symbology(StrictBaseModel):
    type: ColorMapType
    colormap: Optional[Dict[Union[StrictInt, float], Union[RGB, RGBA]]]


def get_source_tile_uris(tiles_geojson_uri):
    s3_client = get_s3_client()
    bucket, key = get_s3_path_parts(tiles_geojson_uri)

    response = s3_client.get_object(Bucket=bucket, Key=key)
    tiles_geojson: Dict[str, Any] = json.loads(response["Body"].read().decode("utf-8"))
    tiles = [
        from_vsi_path(feature["properties"]["name"])
        for feature in tiles_geojson["features"]
    ]
    return tiles


def _sort_colormap(
    no_data_value: Optional[Union[StrictInt, float]],
    symbology: Symbology,
    with_alpha: bool,
) -> OrderedColorMap:
    """
    Create value - quadruplet colormap (GDAL format) including no data value.

    """
    assert symbology.colormap, "No colormap specified."

    colormap: Dict[Union[StrictInt, float], Union[RGB, RGBA]] = copy.deepcopy(
        symbology.colormap
    )

    ordered_gdal_colormap: OrderedColorMap = dict()

    # add no data value to colormap, if exists
    # (not sure why mypy throws an error here, hence type: ignore)
    if no_data_value is not None:
        if with_alpha:
            colormap[no_data_value] = RGBA(red=0, green=0, blue=0, alpha=0)  # type: ignore
        else:
            colormap[no_data_value] = RGB(red=0, green=0, blue=0)  # type: ignore

    # make sure values are correctly sorted and convert to value-quadruplet string
    for pixel_value in sorted(colormap.keys()):
        ordered_gdal_colormap[pixel_value] = colormap[pixel_value].tuple()

    return ordered_gdal_colormap


def create_rgb_tile(
    args: Tuple[str, str, ColorMapType, str, bool, logging.Logger]
) -> str:
    """Add symbology to output raster.

    Gradient colormap: Use linear interpolation based on provided
    colormap to compute RGBA quadruplet for any given pixel value.
    Discrete colormap: Use strict matching when searching in the color
    configuration file. If no matching color entry is found, the
    “0,0,0,0” RGBA quadruplet will be used.
    """
    (
        source_tile_uri,
        target_prefix,
        symbology_type,
        colormap_path,
        add_alpha,
        logger,
    ) = args
    tile_id = os.path.splitext(os.path.basename(source_tile_uri))[0]

    with TemporaryDirectory() as work_dir:
        local_src_file_path = os.path.join(work_dir, os.path.basename(source_tile_uri))
        s3_client = get_s3_client()
        bucket, source_key = get_s3_path_parts(source_tile_uri)

        logger.log(
            logging.INFO, f"Downloading {source_tile_uri} to {local_src_file_path}"
        )
        s3_client.download_file(bucket, source_key, local_src_file_path)

        local_dest_file_path = os.path.join(work_dir, f"{tile_id}_colored.tif")

        block_size = 256  # Block size for WebMercator Tiles is always 256x256

        cmd = [
            "gdaldem",
            "color-relief",
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
        if add_alpha:
            cmd += ["-alpha"]
        if symbology_type in (ColorMapType.discrete, ColorMapType.discrete_intensity):
            cmd += ["-exact_color_entry"]

        cmd += [local_src_file_path, colormap_path, local_dest_file_path]

        logger.log(logging.INFO, f"Running subcommand {cmd}")

        try:
            run_gdal_subcommand(cmd)
        except GDALError:
            logger.log(
                logging.ERROR, f"Could not create Color Relief for tile_id {tile_id}"
            )
            raise

        with rasterio.open(local_dest_file_path, "r+") as src:
            src.nodata = 0

        # Now upload the file to S3
        target_key = os.path.join(target_prefix, os.path.basename(local_src_file_path))
        logger.log(logging.INFO, f"Uploading {local_src_file_path} to {target_key}")
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
    log_queue = multiprocessing.Manager().Queue()
    listener = multiprocessing.Process(
        target=log_listener, args=(log_queue, listener_configurer)
    )
    listener.start()

    log_client_configurer(log_queue)
    logger = logging.getLogger("main")

    logger.log(logging.INFO, f"Applying symbology to tiles listed in {source_uri}")

    tile_uris = get_source_tile_uris(source_uri)

    if not tile_uris:
        logger.log(logging.INFO, "No input files! I guess we're good then.")
        return

    no_data_value: Optional[Union[StrictInt, float]] = json.loads(no_data)

    symbology_obj = Symbology(**json.loads(symbology))

    # If the breakpoints include alpha values, enable the Alpha channel
    assert symbology_obj.colormap is not None
    add_alpha = all(
        isinstance(value, RGBA) for value in symbology_obj.colormap.values()
    )

    ordered_colormap: OrderedColorMap = _sort_colormap(
        no_data_value, symbology_obj, add_alpha
    )

    # TODO: Log the ordered colormap file contents?

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
            (
                tile_uri,
                target_prefix,
                symbology_obj.type,
                colormap_path,
                add_alpha,
                logger,
            )
            for tile_uri in tile_uris
        )

        # Cannot use normal pool here since we run sub-processes
        # https://stackoverflow.com/a/61470465/1410317
        with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
            for tile_id in executor.map(create_rgb_tile, process_args):
                logger.log(logging.INFO, f"Finished processing tile {tile_id}")

    # Now run pixetl_prep.create_geojsons to generate a tiles.geojson and
    # extent.geojson in the target prefix. But that code appends /geotiff
    # to the prefix so remove it first
    create_geojsons_prefix = target_prefix.split(f"{dataset}/{version}/")[1].replace(
        "/geotiff", ""
    )
    logger.log(logging.INFO, "Uploading tiles.geojson to {create_geojsons_prefix}")
    from gfw_pixetl.pixetl_prep import create_geojsons

    create_geojsons(list(), dataset, version, create_geojsons_prefix, True)

    log_queue.put_nowait(None)
    listener.join()


if __name__ == "__main__":
    try:
        run(apply_symbology)
    except (BrokenProcessPool, SubprocessKilledError):
        print("One of our subprocesses was killed! Exiting with 137")
        sys.exit(137)
