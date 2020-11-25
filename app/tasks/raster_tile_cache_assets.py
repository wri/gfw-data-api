import copy
import json
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.logger import logger

from app.crud.assets import create_asset, get_asset
from app.models.enum.assets import AssetType
from app.models.enum.pixetl import ResamplingMethod
from app.models.orm.assets import Asset as ORMAsset
from app.models.pydantic.assets import AssetCreateIn
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import (
    RasterTileSetAssetCreationOptions,
    RasterTileSetSourceCreationOptions,
)
from app.models.pydantic.jobs import BuildRGBJob, GDAL2TilesJob, Job, PixETLJob
from app.settings.globals import (
    AWS_REGION,
    ENV,
    PIXETL_CORES,
    PIXETL_DEFAULT_RESAMPLING,
    PIXETL_MAX_MEM,
    S3_ENTRYPOINT_URL,
    TILE_CACHE_BUCKET,
)
from app.tasks import Callback, callback_constructor, writer_secrets
from app.tasks.batch import execute
from app.utils.path import get_asset_uri

INTENSITY_PIXEL_MEANING = "intensity"
RGB_ENCODED_PIXEL_MEANING = "rgb_encoded"

JOB_ENV = writer_secrets + [
    {"name": "AWS_REGION", "value": AWS_REGION},
    {"name": "ENV", "value": ENV},
    {"name": "CORES", "value": PIXETL_CORES},
    {"name": "MAX_MEM", "value": PIXETL_MAX_MEM},
]

if S3_ENTRYPOINT_URL:
    # Why both? Because different programs (boto,
    # pixetl, gdal*) use different vars.
    JOB_ENV = JOB_ENV + [
        {"name": "AWS_ENDPOINT_URL", "value": S3_ENTRYPOINT_URL},
        {"name": "ENDPOINT_URL", "value": S3_ENTRYPOINT_URL},
    ]


async def raster_tile_cache_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:
    # Argument validation
    min_zoom = input_data["creation_options"]["min_zoom"]
    max_zoom = input_data["creation_options"]["max_zoom"]
    max_static_zoom = input_data["creation_options"]["max_static_zoom"]
    implementation = input_data["creation_options"]["implementation"]

    assert min_zoom <= max_zoom  # FIXME: Raise appropriate exception
    assert max_static_zoom <= max_zoom  # FIXME: Raise appropriate exception

    # FIXME: Remove this when implementing standard tile cache code path:
    if input_data["creation_options"]["symbology"]["type"] != "date_conf_intensity":
        raise NotImplementedError(
            "Raster tile cache currently only implemented for GLAD/RADD pipeline"
        )

    job_list: List[Job] = []

    # source_asset_id is currently required. Could perhaps make it optional
    # in the case that the default asset is the only one.
    source_asset: ORMAsset = await get_asset(
        input_data["creation_options"]["source_asset_id"]
    )

    # We should require that the source asset be of the same dataset
    # and version as the tile cache, right?
    assert source_asset.dataset == dataset  # FIXME: Raise appropriate exception
    assert source_asset.version == version  # FIXME: Raise appropriate exception

    # Re-project the original asset to web-mercator (as new assets)
    # Get the creation options from the original raster tile set asset
    source_asset_co = RasterTileSetSourceCreationOptions(
        **source_asset.creation_options
    )

    # Reset calc to prevent it from interfering with new derivative assets
    # and set source_uri from whatever it is to refer to the raster tile set
    # copy in the data lake
    source_asset_co.calc = None
    source_asset_co.resampling = ResamplingMethod.med
    source_source_uri = get_asset_uri(
        dataset, version, AssetType.raster_tile_set, source_asset_co.dict(by_alias=True)
    ).replace("{tile_id}.tif", "tiles.geojson")
    source_asset_co.source_uri = [source_source_uri]

    source_reprojection_jobs = await _reproject_to_web_mercator(
        dataset,
        version,
        source_asset_co,
        min_zoom,
        max_zoom,
        max_zoom_resampling=PIXETL_DEFAULT_RESAMPLING,
    )
    job_list += source_reprojection_jobs
    logger.debug("SOURCE ASSET REPROJECTION JOBS CREATED")

    # For GLAD/RADD, create intensity asset with pixetl and merge with
    # existing date/conf layer to form a new RGB_ENCODED_PIXEL_MEANING asset
    if input_data["creation_options"]["symbology"]["type"] == "date_conf_intensity":

        # Create intensity asset from date_conf asset creation options
        # No need for a WGS84 copy, so go right to web-mercator
        # date_conf_co_dict = source_asset_co.dict(by_alias=True)
        intensity_source_uri = get_asset_uri(
            dataset,
            version,
            AssetType.raster_tile_set,
            source_asset_co.dict(by_alias=True),
        ).replace("{tile_id}.tif", "tiles.geojson")

        intensity_co_dict = {
            **source_asset_co.dict(by_alias=True),
            **{
                "source_uri": [intensity_source_uri],
                "pixel_meaning": INTENSITY_PIXEL_MEANING,
                "resampling": ResamplingMethod.bilinear,
                "calc": None,
                "grid": source_asset_co.grid,
                "no_data": None,
            },
        }

        intensity_co = RasterTileSetSourceCreationOptions(**intensity_co_dict)
        logger.debug(
            "INTENSITY ASSET CREATION OPTIONS: "
            f"{json.dumps(intensity_co.dict(by_alias=True), indent=2)}"
        )

        intensity_reprojection_jobs = await _reproject_to_web_mercator(
            dataset,
            version,
            intensity_co,
            min_zoom,
            max_zoom,
            max_zoom_calc="(A>0)*55",
            max_zoom_resampling=PIXETL_DEFAULT_RESAMPLING,
        )
        job_list += intensity_reprojection_jobs
        logger.debug("INTENSITY REPROJECTION JOBS CREATED")

        # Now merge the date/conf and intensity tiles for each zoom level to
        # create the final raster tile set asset

        # Create merged asset record
        merge_jobs = await _merge_intensity_and_date_conf(
            dataset,
            version,
            source_asset_co,
            intensity_co,
            min_zoom,
            max_zoom,
            job_list,
        )
        job_list += merge_jobs

        # build_rgb created the merged rasters but not tiles.geojson or extent.geojson
        # FIXME: Create those now with pixetl's pixetl_prep

        # Actually create the tile cache using gdal2tiles
        logger.debug("Now create the tile cache using gdal2tiles...")
        tile_cache_co = intensity_co.dict(by_alias=True)
        tile_cache_co["pixel_meaning"] = RGB_ENCODED_PIXEL_MEANING
        tile_cache_co["srid"] = "epsg-3857"

        tile_cache_jobs = await _create_tile_cache(
            dataset,
            version,
            tile_cache_co,
            min_zoom,
            max_static_zoom,
            implementation,
            callback_constructor(asset_id),
            merge_jobs,
        )
        job_list += tile_cache_jobs

    log: ChangeLog = await execute(job_list)

    return log


async def _run_pixetl(
    dataset: str,
    version: str,
    co: Dict[str, Any],
    job_name: str,
    callback: Callback,
    parents: Optional[List[Job]] = None,
):
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


async def _reproject_to_web_mercator(
    dataset: str,
    version: str,
    source_creation_options: RasterTileSetSourceCreationOptions,
    min_zoom: int,
    max_zoom: int,
    parents: Optional[List[Job]] = None,
    max_zoom_resampling: Optional[str] = None,
    max_zoom_calc: Optional[str] = None,
) -> List[Job]:
    reprojection_jobs: List[Job] = []

    # Processing chokes on large datasets if we go directly to low
    # zoom levels, so start at the highest and work our way back

    co = source_creation_options.dict(by_alias=True)

    for zoom_level in range(max_zoom, min_zoom - 1, -1):

        co["grid"] = f"zoom_{zoom_level}"
        if zoom_level == max_zoom and max_zoom_calc:
            co["calc"] = max_zoom_calc
        if zoom_level == max_zoom and max_zoom_resampling:
            co["resampling"] = max_zoom_resampling

        asset_uri = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, co, "epsg:3857"
        )

        co_obj = RasterTileSetSourceCreationOptions(**co)

        # Create an asset record
        asset_options = AssetCreateIn(
            asset_type=AssetType.raster_tile_set,
            asset_uri=asset_uri,
            is_managed=True,
            creation_options=co_obj,
            metadata={},
        ).dict(by_alias=True)
        wm_asset_record = await create_asset(dataset, version, **asset_options)
        logger.debug(f"ZOOM LEVEL {zoom_level} REPROJECTION ASSET CREATED")

        zoom_level_job = await _run_pixetl(
            dataset,
            version,
            wm_asset_record.creation_options,
            f"zoom_level_{zoom_level}_{co['pixel_meaning']}_reprojection",
            callback_constructor(wm_asset_record.asset_id),
            parents=(parents + reprojection_jobs) if parents else reprojection_jobs,
        )
        reprojection_jobs.append(zoom_level_job)
        logger.debug(f"ZOOM LEVEL {zoom_level} REPROJECTION JOB CREATED")

        # Update creation option for the next iteration of for loop
        co["source_uri"] = [asset_uri.replace("{tile_id}.tif", "tiles.geojson")]
        co["calc"] = source_creation_options.calc
        co["resampling"] = source_creation_options.resampling

    return reprojection_jobs


async def _merge_intensity_and_date_conf(
    dataset: str,
    version: str,
    date_conf_co: RasterTileSetSourceCreationOptions,
    intensity_co: RasterTileSetSourceCreationOptions,
    min_zoom: int,
    max_zoom: int,
    parents: List[Job],
):
    merge_intensity_jobs: List[Job] = []

    for zoom_level in range(min_zoom, max_zoom):
        # Sanitize creation_options

        date_conf_co_dict = date_conf_co.dict(by_alias=True)
        date_conf_co_dict["grid"] = f"zoom_{zoom_level}"
        date_conf_uri = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, date_conf_co_dict, "epsg:3857"
        ).replace("{tile_id}.tif", "tiles.geojson")

        intensity_co_dict = intensity_co.dict(by_alias=True)
        intensity_co_dict["grid"] = f"zoom_{zoom_level}"
        intensity_uri = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, intensity_co_dict, "epsg:3857"
        ).replace("{tile_id}.tif", "tiles.geojson")

        encoded_co_dict = intensity_co.dict(by_alias=True)
        encoded_co_dict["grid"] = f"zoom_{zoom_level}"
        encoded_co_dict["pixel_meaning"] = RGB_ENCODED_PIXEL_MEANING

        asset_uri = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, encoded_co_dict, "epsg:3857"
        )
        encoded_asset_prefix = asset_uri.rsplit("/", 1)[0]

        del encoded_co_dict["source_uri"]
        del encoded_co_dict["source_driver"]
        del encoded_co_dict["source_type"]

        encoded_co = RasterTileSetAssetCreationOptions(**encoded_co_dict)

        logger.debug(
            f"ATTEMPTING TO CREATE MERGED ASSET WITH THESE CREATION OPTIONS: {encoded_co_dict}"
        )

        # Create an asset record
        asset_options = AssetCreateIn(
            asset_type=AssetType.raster_tile_set,
            asset_uri=asset_uri,
            is_managed=True,
            creation_options=encoded_co,
            metadata={},
        ).dict(by_alias=True)

        wm_asset_record = await create_asset(dataset, version, **asset_options)
        logger.debug(
            f"ZOOM LEVEL {zoom_level} MERGED ASSET CREATED WITH ASSET_ID {wm_asset_record.asset_id}"
        )

        command = [
            "merge_intensity.sh",
            date_conf_uri,
            intensity_uri,
            encoded_asset_prefix,
        ]

        callback = callback_constructor(wm_asset_record.asset_id)

        merge_intensity_job = BuildRGBJob(
            job_name=f"merge_intensity_zoom_{zoom_level}",
            command=command,
            environment=JOB_ENV,
            callback=callback,
            parents=[parent.job_name for parent in parents],
        )
        merge_intensity_jobs += [merge_intensity_job]

    return merge_intensity_jobs


async def _create_tile_cache(
    dataset: str,
    version: str,
    r_t_s_creation_options: Dict[str, Any],
    min_zoom: int,
    max_zoom: int,
    implementation: str,
    callback: Callback,
    parents: List[Job],
):
    tile_cache_jobs: List[Job] = []

    for zoom_level in range(min_zoom, max_zoom):
        co = copy.deepcopy(r_t_s_creation_options)
        co["grid"] = f"zoom_{zoom_level}"
        asset_prefix = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, co, co["srid"]
        ).rsplit("/", 1)[0]

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
        tile_cache_jobs += [tile_cache_job]

    return tile_cache_jobs


async def raster_tile_cache_validator(
    dataset: str, version: str, input_data: Dict[str, Any]
) -> None:
    source_asset: ORMAsset = await get_asset(
        input_data["creation_options"]["source_asset_id"]
    )
    if (source_asset.dataset != dataset) or (source_asset.version != version):
        raise HTTPException(
            status_code=400,
            detail="Dataset and version of source asset must match dataset and version of current asset.",
        )
