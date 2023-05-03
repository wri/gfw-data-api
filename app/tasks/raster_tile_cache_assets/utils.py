import math
import os
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from fastapi.logger import logger

from app.crud.assets import create_asset
from app.models.enum.assets import AssetType
from app.models.enum.pixetl import DataType
from app.models.pydantic.assets import AssetCreateIn
from app.models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from app.models.pydantic.jobs import GDAL2TilesJob, Job
from app.models.pydantic.statistics import RasterStats
from app.settings.globals import (
    DEFAULT_JOB_DURATION,
    MAX_CORES,
    MAX_MEM,
    TILE_CACHE_BUCKET,
)
from app.tasks import Callback, callback_constructor
from app.tasks.raster_tile_set_assets.utils import (
    JOB_ENV,
    create_pixetl_job,
    create_resample_job,
)
from app.tasks.utils import sanitize_batch_job_name
from app.utils.path import get_asset_uri, tile_uri_to_tiles_geojson


async def reproject_to_web_mercator(
    dataset: str,
    version: str,
    source_creation_options: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    parents: Optional[List[Job]] = None,
    max_zoom_resampling: Optional[str] = None,
    max_zoom_calc: Optional[str] = None,
    use_resampler: bool = False,
) -> Tuple[Job, str]:
    """Create Tileset reprojected into Web Mercator projection."""

    calc = (
        max_zoom_calc
        if zoom_level == max_zoom and max_zoom_calc
        else source_creation_options.calc
    )
    resampling = (
        max_zoom_resampling
        if zoom_level == max_zoom and max_zoom_resampling
        else source_creation_options.resampling
    )
    source_uri: Optional[List[str]] = get_zoom_source_uri(
        dataset, version, source_creation_options, zoom_level, max_zoom
    )

    # We create RGBA image in a second step, since we cannot easily resample RGBA to next zoom level using PixETL.
    symbology = None

    creation_options = source_creation_options.copy(
        deep=True,
        update={
            "calc": calc,
            "resampling": resampling,
            "grid": f"zoom_{zoom_level}",
            "source_uri": source_uri,
            "symbology": symbology,
        },
    )

    job_name = sanitize_batch_job_name(
        f"{dataset}_{version}_{source_creation_options.pixel_meaning}_{zoom_level}"
    )

    return await create_wm_tile_set_job(
        dataset,
        version,
        creation_options,
        job_name,
        parents,
        use_resampler=use_resampler,
    )


async def create_wm_tile_set_job(
    dataset: str,
    version: str,
    creation_options: RasterTileSetSourceCreationOptions,
    job_name: str,
    parents: Optional[List[Job]] = None,
    use_resampler: bool = False,
) -> Tuple[Job, str]:

    asset_uri = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        creation_options.dict(by_alias=True),
        "epsg:3857",
    )

    # Create an asset record
    asset_options = AssetCreateIn(
        asset_type=AssetType.raster_tile_set,
        asset_uri=asset_uri,
        is_managed=True,
        creation_options=creation_options,
    ).dict(by_alias=True)
    wm_asset_record = await create_asset(dataset, version, **asset_options)

    logger.debug(f"Created asset for {asset_uri}")

    # TODO: Consider removing the use_resampler argument and changing this
    # to "if creation_options.calc is None:"
    # Make sure to test different scenarios when done!
    if use_resampler:
        job = await create_resample_job(
            dataset,
            version,
            creation_options,
            int(creation_options.grid.strip("zoom_")),
            job_name,
            callback_constructor(wm_asset_record.asset_id),
            parents=parents,
        )
    else:
        job = await create_pixetl_job(
            dataset,
            version,
            creation_options,
            job_name,
            callback_constructor(wm_asset_record.asset_id),
            parents=parents,
        )

    zoom_level = int(creation_options.grid.strip("zoom_"))
    job = scale_batch_job(job, zoom_level)

    return job, asset_uri


async def create_tile_cache(
    dataset: str,
    version: str,
    source_uri: str,
    zoom_level: int,
    implementation: str,
    callback: Callback,
    parents: List[Job],
    bit_depth: int,
):
    """Create batch job to generate raster tile cache for given zoom level."""

    asset_prefix = os.path.dirname(source_uri)

    logger.debug(
        f"CREATING TILE CACHE JOB FOR ZOOM LEVEL {zoom_level} WITH PREFIX {asset_prefix}"
    )

    cmd: List[str] = [
        "raster_tile_cache.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-I",
        implementation,
        "--target_bucket",
        TILE_CACHE_BUCKET,
        "--zoom_level",
        str(zoom_level),
        "--bit_depth",
        str(bit_depth),
    ]
    # TODO: GTC-1090, GTC 1091
    #  this should be the default. However there seems to be an issue
    #  with some of the symbology function (discrete, date-conf-intensity)
    #  which generate empty tiles at least during tests.
    #
    if zoom_level > 9:
        cmd += ["--skip"]

    cmd += [asset_prefix]

    tile_cache_job = GDAL2TilesJob(
        dataset=dataset,
        job_name=f"generate_tile_cache_zoom_{zoom_level}",
        command=cmd,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents],
    )

    tile_cache_job = scale_batch_job(tile_cache_job, zoom_level)

    return tile_cache_job


def get_zoom_source_uri(
    dataset: str,
    version: str,
    creation_options: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
) -> Optional[List[str]]:
    """Return URI specified in creation options for highest zoom level,
    otherwise return URI of same tileset but one zoom level up."""

    alternate_source_uri = [
        get_asset_uri(
            dataset,
            version,
            AssetType.raster_tile_set,
            {
                "grid": f"zoom_{zoom_level + 1}",
                "pixel_meaning": creation_options.pixel_meaning,
            },
            "epsg:3857",
        )
    ]

    source_uri = (
        creation_options.source_uri if zoom_level == max_zoom else alternate_source_uri
    )

    return (
        [tile_uri_to_tiles_geojson(uri) for uri in source_uri] if source_uri else None
    )


def generate_stats(stats) -> RasterStats:
    if stats is None:
        raise NotImplementedError()
    return RasterStats(**stats)


def convert_float_to_int(
    stats: Optional[Dict[str, Any]],
    source_asset_co: RasterTileSetSourceCreationOptions,
) -> Tuple[RasterTileSetSourceCreationOptions, str]:

    stats = generate_stats(stats)

    logger.info("In convert_float_to_int()")

    assert len(stats.bands) == 1
    stats_min = stats.bands[0].min
    stats_max = stats.bands[0].max
    value_range = math.fabs(stats_max - stats_min)

    logger.info(
        f"stats_min: {stats_min} stats_max: {stats_max} value_range: {value_range}"
    )

    # Shift by 1 (and add 1 later) so any values of zero don't get counted as no_data
    uint16_max = np.iinfo(np.uint16).max - 1
    # Expand or squeeze to fit into a uint16
    mult_factor = (uint16_max / value_range) if value_range else 1

    logger.info(f"Multiplicative factor: {mult_factor}")

    if isinstance(source_asset_co.no_data, list):
        raise RuntimeError("Cannot apply colormap on multi band image")
    elif source_asset_co.no_data is None:
        old_no_data: str = "None"
    elif source_asset_co.no_data == str(np.nan):
        old_no_data = "np.nan"
    else:
        old_no_data = str(source_asset_co.no_data)

    calc_str = (
        f"(A != {old_no_data}).astype(bool) * "
        f"(1 + (A - {stats_min}) * {mult_factor}).astype(np.uint16)"
    )

    logger.info(f"Resulting calc string: {calc_str}")

    source_asset_co.data_type = DataType.uint16
    source_asset_co.no_data = 0

    if source_asset_co.symbology and source_asset_co.symbology.colormap is not None:
        source_asset_co.symbology.colormap = {
            (1 + (float(k) - stats_min) * mult_factor): v
            for k, v in source_asset_co.symbology.colormap.items()
        }
        logger.info(f"Resulting colormap: {source_asset_co.symbology.colormap}")

    return source_asset_co, calc_str


def scale_batch_job(job: Job, zoom_level: int):
    """Use up to maximum resources for higher and scale down for lower zoom
    levels."""
    if job.num_processes is None:
        job.num_processes = job.vcpus

    cpu_proc_ratio = job.vcpus / job.num_processes

    job.vcpus = min(job.vcpus, math.ceil(2 ** (zoom_level - 4)))
    job.memory = (MAX_MEM / MAX_CORES) * job.vcpus
    job.num_processes = max(1, int(job.vcpus / cpu_proc_ratio))

    if job.attempt_duration_seconds is None:
        job.attempt_duration_seconds = max(
            DEFAULT_JOB_DURATION, int(DEFAULT_JOB_DURATION * (zoom_level / 3))
        )

    return job
