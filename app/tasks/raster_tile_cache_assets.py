import copy
import json
import os
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple
from uuid import UUID

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.logger import logger

from app.crud.assets import create_asset, get_asset
from app.models.enum.assets import AssetType
from app.models.enum.pixetl import ResamplingMethod
from app.models.enum.symbology import ColorMapType
from app.models.orm.assets import Asset as ORMAsset
from app.models.pydantic.assets import AssetCreateIn
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import (
    RasterTileSetAssetCreationOptions,
    RasterTileSetSourceCreationOptions,
)
from app.models.pydantic.jobs import BuildRGBJob, GDAL2TilesJob, Job, PixETLJob
from app.models.pydantic.symbology import Symbology
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
    """Generate Raster Tile Cache Assets."""

    TYPE_SPECIFIC_ZOOM_LEVEL_FUNCTIONS = {
        "date_conf_intensity": _date_conf_intensity,
        "gradient": _gradient,
    }

    min_zoom = input_data["creation_options"]["min_zoom"]
    max_zoom = input_data["creation_options"]["max_zoom"]
    max_static_zoom = input_data["creation_options"]["max_static_zoom"]
    implementation = input_data["creation_options"]["implementation"]
    symbology = input_data["creation_options"]["symbology"]

    # source_asset_id is currently required. Could perhaps make it optional
    # in the case that the default asset is the only one.
    source_asset: ORMAsset = await get_asset(
        input_data["creation_options"]["source_asset_id"]
    )
    # Get the creation options from the original raster tile set asset
    source_asset_co = RasterTileSetSourceCreationOptions(
        **source_asset.creation_options
    )

    source_asset_co.calc = None
    source_asset_co.source_uri = [
        get_asset_uri(
            dataset,
            version,
            AssetType.raster_tile_set,
            source_asset_co.dict(by_alias=True),
        ).replace("{tile_id}.tif", "tiles.geojson")
    ]
    source_asset_co.symbology = Symbology(**symbology)

    job_list: List[Job] = []
    jobs_dict: Dict[int, Dict[str, Job]] = dict()

    type_specific_function = TYPE_SPECIFIC_ZOOM_LEVEL_FUNCTIONS.get(
        symbology["type"], _no_specific_function
    )

    for zoom_level in range(max_zoom, min_zoom - 1, -1):
        jobs_dict[zoom_level] = dict()
        source_projection_parent_job = jobs_dict.get(zoom_level + 1, {}).get(
            "source_reprojection_job"
        )
        source_reprojection_job: Job = await _reproject_to_web_mercator(
            dataset,
            version,
            source_asset_co,
            zoom_level,
            max_zoom,
            source_projection_parent_job,
            max_zoom_resampling=PIXETL_DEFAULT_RESAMPLING,
        )
        jobs_dict[zoom_level]["source_reprojection_job"] = source_reprojection_job
        job_list.append(source_reprojection_job)

        type_specific_jobs: List[Job]
        type_specific_source: str

        type_specific_jobs, type_specific_source = await type_specific_function(
            dataset, version, source_asset_co, zoom_level, max_zoom, jobs_dict,
        )
        job_list += type_specific_jobs

        if zoom_level <= max_static_zoom:
            tile_cache_job: Job = await _create_tile_cache(
                dataset,
                version,
                type_specific_source,
                zoom_level,
                implementation,
                callback_constructor(asset_id),
                type_specific_jobs + [source_reprojection_job],
            )
            job_list.append(tile_cache_job)

    log: ChangeLog = await execute(job_list)
    return log


async def raster_tile_cache_validator(
    dataset: str, version: str, input_data: Dict[str, Any]
) -> None:
    """Validate Raster Tile Cache Creation Options.

    Used in asset route. If validation fails, it will raise a
    HTTPException visible to user.
    """
    source_asset: ORMAsset = await get_asset(
        input_data["creation_options"]["source_asset_id"]
    )
    if (source_asset.dataset != dataset) or (source_asset.version != version):
        raise HTTPException(
            status_code=400,
            detail="Dataset and version of source asset must match dataset and version of current asset.",
        )


async def _run_pixetl(
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


async def _reproject_to_web_mercator(
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

    zoom_level_job = await _run_pixetl(
        dataset,
        version,
        wm_asset_record.creation_options,
        f"zoom_level_{zoom_level}_{co.pixel_meaning}_reprojection",
        callback_constructor(wm_asset_record.asset_id),
        parents=[parent] if parent else None,
    )
    logger.debug(f"ZOOM LEVEL {zoom_level} REPROJECTION JOB CREATED")

    return zoom_level_job


async def _create_tile_cache(
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


async def _no_specific_function(
    dataset: str,
    version: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    if source_asset_co.source_uri:
        return list(), source_asset_co.source_uri[0]
    else:
        raise RuntimeError("No source URI set.")


async def _gradient(
    dataset: str,
    version: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    raise NotImplementedError("Gradient symbology not implemented")


async def _date_conf_intensity(
    dataset: str,
    version: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    INTENSITY_PIXEL_MEANING = "intensity"

    source_uri = (
        source_asset_co.source_uri
        if not zoom_level == max_zoom
        else [
            get_asset_uri(
                dataset,
                version,
                AssetType.raster_tile_set,
                source_asset_co.dict(by_alias=True),
            ).replace("{tile_id}.tif", "tiles.geojson")
        ]
    )
    co = source_asset_co.copy(
        deep=True,
        update={
            "source_uri": source_uri,
            "no_data": None,
            "pixel_meaning": INTENSITY_PIXEL_MEANING,
        },
    )

    previous_level_intensity_reprojection_job: Optional[Job] = None
    if zoom_level != max_zoom:
        previous_level_intensity_reprojection_job = jobs_dict[zoom_level + 1][
            "intensity_reprojection_job"
        ]

    intensity_reprojection_job: Job = await _reproject_to_web_mercator(
        dataset,
        version,
        co,
        zoom_level,
        max_zoom,
        previous_level_intensity_reprojection_job,
        max_zoom_resampling=PIXETL_DEFAULT_RESAMPLING,
        max_zoom_calc="(A>0)*55",
    )
    jobs_dict[zoom_level]["intensity_reprojection_job"] = intensity_reprojection_job

    source_reprojection_job = jobs_dict[zoom_level]["source_reprojection_job"]

    intensity_co = source_asset_co.copy(
        deep=True,
        update={"pixel_meaning": INTENSITY_PIXEL_MEANING, "grid": f"zoom_{zoom_level}"},
    )

    source_co = source_asset_co.copy(deep=True, update={"grid": f"zoom_{zoom_level}"})

    merge_job, dst_uri = await _merge_intensity_and_date_conf(
        dataset,
        version,
        source_co,
        intensity_co,
        zoom_level,
        [source_reprojection_job, intensity_reprojection_job],
    )
    jobs_dict[zoom_level]["merge_intensity_job"] = merge_job

    return [intensity_reprojection_job, merge_job], dst_uri


async def _merge_intensity_and_date_conf(
    dataset: str,
    version: str,
    date_conf_co: RasterTileSetSourceCreationOptions,
    intensity_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    parents: List[Job],
) -> Tuple[Job, str]:
    FINAL_PIXEL_MEANING = "rgb_encoded"

    date_conf_uri = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        date_conf_co.dict(by_alias=True),
        "epsg:3857",
    ).replace("{tile_id}.tif", "tiles.geojson")

    intensity_uri = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        intensity_co.dict(by_alias=True),
        "epsg:3857",
    ).replace("{tile_id}.tif", "tiles.geojson")

    encoded_co_dict = intensity_co.dict(by_alias=True)
    encoded_co_dict["pixel_meaning"] = FINAL_PIXEL_MEANING

    merged_asset_uri = get_asset_uri(
        dataset, version, AssetType.raster_tile_set, encoded_co_dict, "epsg:3857"
    )
    merged_asset_prefix = merged_asset_uri.rsplit("/", 1)[0]

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
        asset_uri=merged_asset_uri,
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
        merged_asset_prefix,
    ]

    callback = callback_constructor(wm_asset_record.asset_id)

    merge_intensity_job = BuildRGBJob(
        job_name=f"merge_intensity_zoom_{zoom_level}",
        command=command,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents],
    )

    return merge_intensity_job, os.path.join(merged_asset_prefix, "tiles.geojson")
