import copy
import json
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi.encoders import jsonable_encoder

from app.crud.assets import create_asset, get_asset, get_default_asset
from app.models.enum.assets import AssetType
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
    PIXETL_MAX_MEM,
    S3_ENTRYPOINT_URL,
    TILE_CACHE_BUCKET,
)
from app.tasks import Callback, callback_constructor, writer_secrets
from app.tasks.batch import execute
from app.utils.path import get_asset_uri

job_env = writer_secrets + [
    {"name": "AWS_REGION", "value": AWS_REGION},
    {"name": "ENV", "value": ENV},
    {"name": "CORES", "value": PIXETL_CORES},
    {"name": "MAX_MEM", "value": PIXETL_MAX_MEM},
]

if S3_ENTRYPOINT_URL:
    job_env = job_env + [
        {"name": "AWS_S3_ENDPOINT", "value": S3_ENTRYPOINT_URL},
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

    assert min_zoom <= max_zoom  # FIXME: Raise appropriate exception
    assert max_static_zoom <= max_zoom  # FIXME: Raise appropriate exception

    job_list: List[Job] = []

    # source_asset_id is currently required. Could perhaps make it optional
    # in the case that the default asset is the only raster tile set.
    source_asset: ORMAsset = await get_asset(
        input_data["creation_options"]["source_asset_id"]
    )

    # We should require that the source asset be of the same dataset
    # and version as the tile cache, right?
    assert source_asset.dataset == dataset  # FIXME: Raise appropriate exception
    assert source_asset.version == version  # FIXME: Raise appropriate exception

    # For GLAD/RADD, create intensity asset with pixetl and merge with
    # existing date_conf layer to form a new "rgb_encoded" asset
    if input_data["creation_options"]["use_intensity"]:

        # Get the creation options from the original date_conf asset
        date_conf_co = RasterTileSetSourceCreationOptions(
            **source_asset.creation_options
        ).dict(by_alias=True)
        print(f"DATE_CONF CREATION OPTIONS: {json.dumps(date_conf_co, indent=2)}")

        # Create intensity asset from date_conf asset creation options
        intensity_co = RasterTileSetSourceCreationOptions(
            **{
                "source_driver": date_conf_co["source_driver"],
                "source_type": "raster",
                "source_uri": [
                    get_asset_uri(
                        dataset, version, AssetType.raster_tile_set, date_conf_co
                    ).replace("{tile_id}.tif", "tiles.geojson")
                ],
                "pixel_meaning": "intensity",
                "resampling": "med",
                "calc": "(A>0)*55",
                "data_type": date_conf_co["data_type"],
                "grid": date_conf_co["grid"],
                "nbits": date_conf_co["nbits"],
                "no_data": date_conf_co["no_data"],
                "overwrite": True,
                "subset": date_conf_co["subset"],
            }
        )
        print(
            f"INTENSITY CREATION OPTIONS: {json.dumps(intensity_co.dict(by_alias=True), indent=2)}"
        )

        # Create intensity asset record in DB
        asset_options = AssetCreateIn(
            asset_type=AssetType.raster_tile_set,
            asset_uri=get_asset_uri(
                dataset,
                version,
                AssetType.raster_tile_set,
                intensity_co.dict(by_alias=True),
            ),
            is_managed=True,
            creation_options=intensity_co,
            metadata={},
        ).dict(by_alias=True)
        intensity_asset_record = await create_asset(dataset, version, **asset_options)
        print("INTENSITY ASSET CREATED")

        # DEBUGGING:
        intensity_asset_record_co = RasterTileSetSourceCreationOptions(
            **intensity_asset_record.creation_options
        ).dict(by_alias=True, exclude_none=True)
        print(
            f"INTENSITY_CO AS STORED IN RECORD: {json.dumps(intensity_asset_record_co, indent=2)}"
        )

        intensity_job = await _run_pixetl(
            dataset,
            version,
            intensity_asset_record.creation_options,
            "create_intensity",
            callback_constructor(intensity_asset_record.asset_id),
        )
        job_list.append(intensity_job)
        print("INTENSITY JOB CREATED")

        # Re-project date_conf and intensity to web mercator with pixetl
        # Note that technically the date_conf reprojection jobs don't need
        # to depend on the intensity job, think about separating this for
        # loop later
        for co in (copy.deepcopy(date_conf_co), intensity_co.dict(by_alias=True)):
            more_jobs = await _reproject_to_web_mercator(
                dataset, version, co, min_zoom, max_static_zoom, [intensity_job],
            )
            job_list += more_jobs
        print("REPROJECTION JOBS CREATED")

        # Now merge the date_conf and intensity tiles for each zoom level to
        # create the final raster tile set asset

        # Create merged asset record
        merge_jobs = await _merge_intensity_and_date_conf(
            dataset,
            version,
            copy.deepcopy(date_conf_co),
            intensity_co.dict(by_alias=True),
            min_zoom,
            max_static_zoom,
            job_list,
        )
        job_list += merge_jobs

        # FIXME: build_rgb created the merged tiles but not tiles.geojson or extent.geojson
        # Create those now with pixetl's pixetl_prep?

        # Actually create the tile cache using gdal2tiles
        print("Now create the tile cache using gdal2tiles...")
        tile_cache_jobs = await _create_tile_cache(
            dataset,
            version,
            intensity_co.dict(by_alias=True),
            min_zoom,
            max_static_zoom,
            callback_constructor(asset_id),
            job_list,
        )
        job_list += tile_cache_jobs

        print("Yup, made it here")
        print(f"JOB LIST LENGTH SO FAR: {len(job_list)}")
        for job in job_list:
            print(f"JOB: {job.job_name}")
            print(f"  PARENTS: {job.parents}")

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
    # FIXME: See if "intensity" asset already exists first

    co["source_uri"] = co.pop("source_uri")[0]
    overwrite = co.pop("overwrite", False)  # FIXME: Think about this value some more
    subset = co.pop("subset", None)

    command = [
        "run_pixetl.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-j",
        json.dumps(jsonable_encoder(co)),
    ]

    if overwrite:
        command += ["--overwrite"]

    if subset:
        command += ["--subset", subset]

    pixetl_job_id = PixETLJob(
        job_name=job_name,
        command=command,
        environment=job_env,
        callback=callback,
        parents=[parent.job_name for parent in parents] if parents else None,
    )

    return pixetl_job_id


async def _reproject_to_web_mercator(
    dataset: str,
    version: str,
    source_creation_options: Dict[str, Any],
    min_zoom: int,
    max_zoom: int,
    parents: Optional[List[Job]],
) -> List[Job]:
    job_list: List[Job] = []

    for zoom_level in range(min_zoom, max_zoom):
        # Sanitize creation_options
        co = copy.deepcopy(source_creation_options)
        source_uri = get_asset_uri(dataset, version, AssetType.raster_tile_set, co)
        co["srid"] = "epsg-3857"
        co["source_uri"] = [source_uri.replace("{tile_id}.tif", "tiles.geojson")]
        co["calc"] = None
        co["grid"] = f"zoom_{zoom_level}"
        co["resampling"] = "med"
        co["overwrite"] = True  # FIXME: Grab from date_conf, default to False

        asset_uri = get_asset_uri(dataset, version, AssetType.raster_tile_set, co)

        del co["srid"]

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
        print(f"ZOOM LEVEL {zoom_level} REPROJECTION ASSET CREATED")

        zoom_level_job = await _run_pixetl(
            dataset,
            version,
            wm_asset_record.creation_options,
            f"zoom_level_{zoom_level}_{co['pixel_meaning']}_reprojection",
            callback_constructor(wm_asset_record.asset_id),
            parents=parents,
        )
        job_list.append(zoom_level_job)
        print(f"ZOOM LEVEL {zoom_level} REPROJECTION JOB CREATED")

    return job_list


async def _merge_intensity_and_date_conf(
    dataset: str,
    version: str,
    date_conf_co: Dict[str, Any],
    intensity_co: Dict[str, Any],
    min_zoom: int,
    max_zoom: int,
    parents: List[Job],
):
    # BLAH BLAH BLAH
    merge_intensity_jobs: List[Job] = []

    for zoom_level in range(min_zoom, max_zoom):
        # Sanitize creation_options

        d_c_co = copy.deepcopy(date_conf_co)
        d_c_co["srid"] = "epsg-3857"
        d_c_co["grid"] = f"zoom_{zoom_level}"
        date_conf_uri = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, d_c_co
        ).replace("{tile_id}.tif", "tiles.geojson")

        i_co = copy.deepcopy(intensity_co)
        i_co["srid"] = "epsg-3857"
        i_co["grid"] = f"zoom_{zoom_level}"
        intensity_uri = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, i_co
        ).replace("{tile_id}.tif", "tiles.geojson")

        c_co = copy.deepcopy(intensity_co)
        c_co["srid"] = "epsg-3857"
        c_co["grid"] = f"zoom_{zoom_level}"
        c_co["pixel_meaning"] = "rgb_encoded"
        asset_uri = get_asset_uri(dataset, version, AssetType.raster_tile_set, c_co)
        merged_asset_prefix = asset_uri.rsplit("/", 1)[0]

        del c_co["source_uri"]
        del c_co["source_driver"]
        del c_co["source_type"]
        del c_co["srid"]

        co_obj = RasterTileSetAssetCreationOptions(**c_co)

        print(f"ATTEMPTING TO CREATE MERGED ASSET WITH THESE CREATION OPTIONS: {c_co}")

        # Create an asset record
        asset_options = AssetCreateIn(
            asset_type=AssetType.raster_tile_set,
            asset_uri=asset_uri,
            is_managed=True,
            creation_options=co_obj,
            metadata={},
        ).dict(by_alias=True)

        wm_asset_record = await create_asset(dataset, version, **asset_options)
        print(
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
            environment=job_env,
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
    callback: Callback,
    parents: List[Job],
):
    # BLAH BLAH BLAH
    tile_cache_jobs: List[Job] = []

    for zoom_level in range(min_zoom, max_zoom):
        # Sanitize creation_options

        co = copy.deepcopy(r_t_s_creation_options)
        co["srid"] = "epsg-3857"
        co["grid"] = f"zoom_{zoom_level}"
        co["pixel_meaning"] = "rgb_encoded"
        asset_prefix = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, co
        ).rsplit("/", 1)[0]

        print(
            f"CREATING TILE CACHE JOB FOR ZOOM LEVEL {zoom_level} WITH PREFIX {asset_prefix}"
        )

        command: List[str] = [
            "raster_tile_cache.sh",
            "-d",
            dataset,
            "-v",
            version,
            "--target_bucket",
            TILE_CACHE_BUCKET,
            "--zoom_level",
            str(zoom_level),
            asset_prefix,
        ]

        tile_cache_job = GDAL2TilesJob(
            job_name=f"generate_tile_cache_zoom_{zoom_level}",
            command=command,
            environment=job_env,
            callback=callback,
            parents=[parent.job_name for parent in parents],
        )
        tile_cache_jobs += [tile_cache_job]

    return tile_cache_jobs
