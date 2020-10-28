from typing import Any, Dict, List
from uuid import UUID

from app.crud.assets import get_default_asset
from app.models.orm.assets import Asset
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.jobs import PixETLJob
from app.settings.globals import ENV, S3_ENTRYPOINT_URL
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

    layer_def = {
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
        layer_def,
    ]

    if overwrite:
        command += ["--overwrite"]

    if subset:
        command += ["--subset", subset]

    create_raster_tile_set_job = PixETLJob(
        job_name="create_intensity_layer",
        command=command,
        environment=job_env,
        callback=callback,
    )

    return create_raster_tile_set_job


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

    # For GLAD/RADD, create intensity asset with pixetl
    if input_data["creation_options"]["use_intensity"]:
        # Get pixetl settings from the (raster tile set) default asset's creation options
        default_asset = await get_default_asset(dataset, version)
        intensity_job = await _generate_intensity_asset(
            dataset, version, default_asset, job_env, callback
        )
        job_list.append(intensity_job)

    log: ChangeLog = await execute(job_list)

    # re-project date_conf and intensity with pixetl
    # merge intensity and date_conf into single asset using build_rgb

    # actually create the tile cache using gdal2tiles

    return log
