import os
from typing import List, Optional, Tuple

from fastapi.logger import logger

from app.crud.assets import create_asset
from app.models.enum.assets import AssetType
from app.models.pydantic.assets import AssetCreateIn
from app.models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from app.models.pydantic.jobs import GDAL2TilesJob, Job
from app.models.pydantic.metadata import RasterTileSetMetadata
from app.settings.globals import TILE_CACHE_BUCKET
from app.tasks import Callback, callback_constructor
from app.tasks.raster_tile_set_assets.utils import JOB_ENV, create_pixetl_job
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
        dataset, version, creation_options, job_name, parents
    )


async def create_wm_tile_set_job(
    dataset: str,
    version: str,
    creation_options: RasterTileSetSourceCreationOptions,
    job_name: str,
    parents: Optional[List[Job]] = None,
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
        metadata=RasterTileSetMetadata(),
    ).dict(by_alias=True)
    wm_asset_record = await create_asset(dataset, version, **asset_options)

    logger.debug(f"Created asset for {asset_uri}")

    job = await create_pixetl_job(
        dataset,
        version,
        wm_asset_record.creation_options,
        job_name,
        callback_constructor(wm_asset_record.asset_id),
        parents=parents,
    )

    return job, asset_uri


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


def get_zoom_source_uri(
    dataset: str,
    version: str,
    creation_options: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
) -> Optional[List[str]]:
    """Use uri specified in creation option for highest zoom level, otherwise
    use uri of same tileset but one zoom level up."""

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
