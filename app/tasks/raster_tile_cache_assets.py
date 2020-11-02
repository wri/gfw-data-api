import json
from typing import Any, Dict, List
from uuid import UUID

from fastapi.encoders import jsonable_encoder

from app.crud.assets import get_default_asset
from app.models.orm.assets import Asset
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.jobs import BuildRGBJob, PixETLJob
from app.settings.globals import DATA_LAKE_BUCKET, ENV, S3_ENTRYPOINT_URL
from app.tasks import Callback, callback_constructor, writer_secrets
from app.tasks.batch import execute


async def _generate_intensity_asset(
    dataset: str,
    version: str,
    ormasset: Asset,
    job_env: List[Dict[str, str]],
    callback: Callback,
):
    co = ormasset.creation_options

    # FIXME: Create an Asset in the DB to track intensity asset in S3

    # FIXME: Hard-code for the moment
    source_uri = f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/raster/epsg-4326/{co['grid']}/date_conf/geotiff/tiles.geojson"

    layer_def = {
        "source_uri": source_uri,
        "source_type": co["source_type"],
        "data_type": co["data_type"],
        "pixel_meaning": "intensity",
        "grid": co["grid"],
        "resampling": co["resampling"],
        "nbits": co["nbits"],
        "no_data": co["no_data"],
        "calc": "(A>0)*55",
    }

    overwrite = True  # FIXME: Think about this value some more
    subset = co["subset"]

    command = [
        "create_raster_tile_set.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-j",
        json.dumps(jsonable_encoder(layer_def)),
    ]

    if overwrite:
        command += ["--overwrite"]

    if subset:
        command += ["--subset", subset]

    create_intensity_job = PixETLJob(
        job_name="create_intensity_layer",
        command=command,
        environment=job_env,
        callback=callback,
    )

    return create_intensity_job


# async def _merge_intensity_and_date_conf(
#     dataset: str,
#     version: str,
#     date_conf_uri: str,
#     intensity_uri: str,
#     job_env: List[Dict[str, str]],
#     callback: Callback,
# ):
#
#     command = [
#         "merge_intensity.sh",
#         "-d",
#         dataset,
#         "-v",
#         version,
#         date_conf_uri,
#         intensity_uri
#     ]
#
#     merge_intensity_job = BuildRGBJob(
#         job_name="merge_intensity_and_date_conf_assets",
#         command=command,
#         environment=job_env,
#         callback=callback,
#     )
#
#     return merge_intensity_job


async def raster_tile_cache_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:
    # Argument validation
    min_zoom = input_data["creation_options"]["min_zoom"]
    max_zoom = input_data["creation_options"]["max_zoom"]
    assert min_zoom <= max_zoom  # FIXME: Raise appropriate exception

    # What is needed to create a raster tile cache?
    # Should default asset be a raster tile set? Is it enough that
    # ANY ASSET is a raster tile set?

    callback: Callback = callback_constructor(asset_id)

    job_env: List[Dict[str, str]] = writer_secrets + [{"name": "ENV", "value": ENV}]
    if S3_ENTRYPOINT_URL:
        job_env = job_env + [{"name": "AWS_S3_ENDPOINT", "value": S3_ENTRYPOINT_URL}]

    job_list = []

    # For GLAD/RADD, create intensity asset with pixetl and merge with
    # existing date_conf layer to form a new asset
    if input_data["creation_options"]["use_intensity"]:
        # Get pixetl settings from the (raster tile set) default asset's creation options
        default_asset = await get_default_asset(dataset, version)
        intensity_job = await _generate_intensity_asset(
            dataset, version, default_asset, job_env, callback
        )
        job_list.append(intensity_job)

        # Merge intensity and date_conf into a single asset using build_rgb
        # FIXME: Hard-coding these for the moment:
        srid: str = "epsg-4326"
        grid: str = "90/27008"
        date_conf_uri: str = f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/raster/{srid}/{grid}/date_conf/geotiff/tiles.geojson"
        intensity_uri: str = f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/raster/{srid}/{grid}/intensity/geotiff/tiles.geojson"

        command = [
            "merge_intensity.sh",
            "-d",
            dataset,
            "-v",
            version,
            date_conf_uri,
            intensity_uri,
        ]

        merge_intensity_job = BuildRGBJob(
            job_name="merge_intensity_and_date_conf_assets",
            command=command,
            environment=job_env,
            parents=[str(intensity_job)],
            callback=callback,
        )
        job_list.append(merge_intensity_job)

    # re-project date_conf and intensity with pixetl

    # actually create the tile cache using gdal2tiles
    print("Now create the tile cache using gdal2tiles...")

    log: ChangeLog = await execute(job_list)

    return log
