import copy
import json
import os
from typing import Any, Dict, List, Optional

from fastapi.encoders import jsonable_encoder
from fastapi.logger import logger

from app.crud.assets import create_asset
from app.models.enum.assets import AssetType
from app.models.enum.symbology import ColorMapType
from app.models.pydantic.assets import AssetCreateIn
from app.models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from app.models.pydantic.jobs import GDAL2TilesJob, Job, PixETLJob
from app.settings.globals import (
    AWS_REGION,
    ENV,
    PIXETL_CORES,
    PIXETL_MAX_MEM,
    S3_ENTRYPOINT_URL,
    TILE_CACHE_BUCKET,
)
from app.tasks import Callback, callback_constructor, writer_secrets
from app.utils.path import get_asset_uri

JOB_ENV = writer_secrets + [
    {"name": "AWS_REGION", "value": AWS_REGION},
    {"name": "ENV", "value": ENV},
    {"name": "CORES", "value": PIXETL_CORES},
    {"name": "MAX_MEM", "value": PIXETL_MAX_MEM},
]

if S3_ENTRYPOINT_URL:
    # Why both? Because different programs (boto,
    # pixetl, gdal*) use different vars.
    JOB_ENV += [
        {"name": "AWS_ENDPOINT_URL", "value": S3_ENTRYPOINT_URL},
        {"name": "ENDPOINT_URL", "value": S3_ENTRYPOINT_URL},
    ]


async def run_pixetl(
    dataset: str,
    version: str,
    co: Dict[str, Any],
    job_name: str,
    callback: Callback,
    parents: Optional[List[Job]] = None,
) -> Job:
    """Schedule a PixETL Batch Job."""
    co_copy = copy.deepcopy(co)
    co_copy["source_uri"] = co_copy.pop("source_uri")[0]
    overwrite = co_copy.pop("overwrite", False)
    subset = co_copy.pop("subset", None)

    command = [
        "run_pixetl.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-j",
        json.dumps(jsonable_encoder(co_copy)),
    ]

    if overwrite:
        command += ["--overwrite"]

    if subset:
        command += ["--subset", subset]

    pixetl_job_id = PixETLJob(
        job_name=job_name,
        command=command,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents] if parents else None,
    )

    return pixetl_job_id


async def reproject_to_web_mercator(
    dataset: str,
    version: str,
    source_creation_options: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    parent: Optional[Job] = None,
    max_zoom_resampling: Optional[str] = None,
    max_zoom_calc: Optional[str] = None,
) -> Job:
    """Create Tileset reprojected into Web Mercator projection."""

    alternate_source_uri = [
        get_asset_uri(
            dataset,
            version,
            AssetType.raster_tile_set,
            {
                "grid": f"zoom_{zoom_level + 1}",
                "pixel_meaning": source_creation_options.pixel_meaning,
            },
            "epsg:3857",
        ).replace("{tile_id}.tif", "tiles.geojson")
    ]
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
    source_uri = (
        source_creation_options.source_uri
        if zoom_level == max_zoom
        else alternate_source_uri
    )
    symbology = (
        source_creation_options.symbology
        if source_creation_options.symbology
        and source_creation_options.symbology.type == ColorMapType.discrete
        else None
    )

    co = source_creation_options.copy(
        deep=True,
        update={
            "calc": calc,
            "resampling": resampling,
            "grid": f"zoom_{zoom_level}",
            "source_uri": source_uri,
            "symbology": symbology,
        },
    )

    asset_uri = get_asset_uri(
        dataset, version, AssetType.raster_tile_set, co.dict(by_alias=True), "epsg:3857"
    )

    # Create an asset record
    asset_options = AssetCreateIn(
        asset_type=AssetType.raster_tile_set,
        asset_uri=asset_uri,
        is_managed=True,
        creation_options=co,
        metadata={},
    ).dict(by_alias=True)
    wm_asset_record = await create_asset(dataset, version, **asset_options)
    logger.debug(f"ZOOM LEVEL {zoom_level} REPROJECTION ASSET CREATED")

    zoom_level_job = await run_pixetl(
        dataset,
        version,
        wm_asset_record.creation_options,
        f"zoom_level_{zoom_level}_{co.pixel_meaning}_reprojection",
        callback_constructor(wm_asset_record.asset_id),
        parents=[parent] if parent else None,
    )
    logger.debug(f"ZOOM LEVEL {zoom_level} REPROJECTION JOB CREATED")

    return zoom_level_job


async def create_tile_cache(
    dataset: str,
    version: str,
    source_uri: str,
    zoom_level: int,
    implementation: str,
    callback: Callback,
    parents: List[Job],
):
    """Create batch job to generate raster tile cache for given zoom level."""

    asset_prefix = os.path.dirname(source_uri)

    logger.debug(
        f"CREATING TILE CACHE JOB FOR ZOOM LEVEL {zoom_level} WITH PREFIX {asset_prefix}"
    )

    command: List[str] = [
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
        asset_prefix,
    ]

    tile_cache_job = GDAL2TilesJob(
        job_name=f"generate_tile_cache_zoom_{zoom_level}",
        command=command,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents],
    )

    return tile_cache_job
