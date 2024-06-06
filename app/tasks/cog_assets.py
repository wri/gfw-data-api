import posixpath
from typing import Any, Callable, Coroutine, Dict
from uuid import UUID

from app.crud.assets import get_asset
from app.models.enum.assets import AssetType
from app.models.enum.pixetl import ResamplingMethod
from app.models.orm.assets import Asset as ORMAsset
from app.models.orm.tasks import Task
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import COGCreationOptions
from app.models.pydantic.jobs import GDALPythonLargeJob, Job
from app.settings.globals import DATA_LAKE_BUCKET
from app.tasks import callback_constructor
from app.tasks.batch import execute
from app.tasks.raster_tile_set_assets.utils import JOB_ENV
from app.tasks.utils import sanitize_batch_job_name
from app.utils.path import get_asset_uri, infer_srid_from_grid, split_s3_path


async def cog_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:
    """Create a COG asset from a raster tile set asset."""

    # Create the Batch job to generate the COG
    creation_options: COGCreationOptions = COGCreationOptions(
        **input_data["creation_options"]
    )

    cog_job: Job = await create_cogify_job(
        dataset,
        version,
        creation_options,
        callback_constructor(asset_id),
    )

    log: ChangeLog = await execute([cog_job])
    return log


async def create_cogify_job(
    dataset: str,
    version: str,
    creation_options: COGCreationOptions,
    callback: Callable[[UUID, ChangeLog], Coroutine[UUID, ChangeLog, Task]],
) -> Job:
    """Create a Batch job to coalesce a raster tile set into a COG.

    For the moment only suitable for EPSG:3857 raster tile sets.
    """
    source_asset: ORMAsset = await get_asset(UUID(creation_options.source_asset_id))

    srid = infer_srid_from_grid(source_asset.creation_options["grid"])
    asset_uri = get_asset_uri(
        dataset, version, AssetType.raster_tile_set, source_asset.creation_options, srid
    )

    # get folder of tiles
    source_uri = "/".join(asset_uri.split("/")[:-1]) + "/"

    # We want to wind up with "{dataset}/{version}/raster/{projection}/cog/{implementation}.tif"
    target_asset_uri = get_asset_uri(
        dataset,
        version,
        AssetType.cog,
        creation_options.dict(by_alias=True),
        srid,
    )

    target_prefix = posixpath.dirname(
        posixpath.dirname(split_s3_path(target_asset_uri)[1])
    )

    # The GDAL utilities use "near" whereas rasterio/pixetl use "nearest"
    resample_method = (
        "near"
        if creation_options.resampling == ResamplingMethod.nearest
        else creation_options.resampling.value
    )

    command = [
        "cogify.sh",
        "-s",
        source_uri,
        "--target_bucket",
        DATA_LAKE_BUCKET,
        "--target_prefix",
        target_prefix,
        "-r",
        resample_method,
        "--block_size",
        creation_options.block_size.value,
        "-T",
        target_prefix,
    ]

    job_name: str = sanitize_batch_job_name(
        f"COGify_{dataset}_{version}_{creation_options.implementation}"
    )

    kwargs = dict()

    return GDALPythonLargeJob(
        dataset=dataset,
        job_name=job_name,
        command=command,
        environment=JOB_ENV,
        callback=callback,
        **kwargs,
    )
