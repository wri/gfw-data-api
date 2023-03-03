import json
import posixpath
from typing import List, Optional

from fastapi.encoders import jsonable_encoder

from app.models.enum.assets import AssetType
from app.models.enum.pixetl import ResamplingMethod
from app.models.pydantic.creation_options import PixETLCreationOptions
from app.models.pydantic.jobs import GDALDEMJob, Job, PixETLJob
from app.settings.globals import (
    AWS_GCS_KEY_SECRET_ARN,
    DEFAULT_JOB_DURATION,
    ENV,
    MAX_CORES,
    MAX_MEM,
    S3_ENTRYPOINT_URL,
)
from app.tasks import Callback, reader_secrets
from app.utils.path import get_asset_uri, split_s3_path, tile_uri_to_tiles_geojson

JOB_ENV = reader_secrets + [
    {"name": "ENV", "value": ENV},
]

if AWS_GCS_KEY_SECRET_ARN:
    JOB_ENV += [{"name": "AWS_GCS_KEY_SECRET_ARN", "value": AWS_GCS_KEY_SECRET_ARN}]


if S3_ENTRYPOINT_URL:
    # Why both? Because different programs (boto,
    # pixetl, gdal*) use different vars.
    JOB_ENV += [
        {"name": "AWS_ENDPOINT_URL", "value": S3_ENTRYPOINT_URL},
        {"name": "ENDPOINT_URL", "value": S3_ENTRYPOINT_URL},
    ]


async def create_pixetl_job(
    dataset: str,
    version: str,
    co: PixETLCreationOptions,
    job_name: str,
    callback: Callback,
    parents: Optional[List[Job]] = None,
) -> Job:
    """Create a Batch job to process a raster tile set using pixetl."""
    co_copy = co.dict(exclude_none=True, by_alias=True)
    overwrite = co_copy.pop("overwrite", False)
    subset = co_copy.pop("subset", None)
    layer_def = json.dumps(jsonable_encoder(co_copy))

    command = [
        "run_pixetl.sh",
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

    kwargs = dict()
    # Experience indicates float jobs take longer...
    if "float" in co.data_type:
        kwargs["attempt_duration_seconds"] = int(DEFAULT_JOB_DURATION * 3)
        kwargs["vcpus"] = MAX_CORES
        kwargs["memory"] = MAX_MEM
        kwargs["num_processes"] = MAX_CORES
        # ...and float64 jobs require twice the memory for the same number of processes.
        # We can't increase the memory anymore, so halve the processes
        if "float64" in co.data_type:
            kwargs["num_processes"] = int(MAX_CORES / 2)

    # ...but allow the user to override parallelism and timeout values
    if co.num_processes is not None:
        kwargs["num_processes"] = co.num_processes
    if co.timeout_sec is not None:
        kwargs["attempt_duration_seconds"] = co.timeout_sec

    return PixETLJob(
        dataset=dataset,
        job_name=job_name,
        command=command,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents] if parents else None,
        **kwargs,
    )


async def create_gdaldem_job(
    dataset: str,
    version: str,
    co: PixETLCreationOptions,
    job_name: str,
    callback: Callback,
    parents: Optional[List[Job]] = None,
):
    """Create a Batch job that applies a colormap to a raster tile set using
    gdaldem."""
    symbology = json.dumps(jsonable_encoder(co.symbology))
    no_data = json.dumps(co.no_data)

    # Possibly not after https://github.com/wri/gfw-data-api/pull/153 ?
    assert isinstance(co.source_uri, List) and len(co.source_uri) == 1
    source_asset_uri = co.source_uri[0]

    target_asset_uri = tile_uri_to_tiles_geojson(
        get_asset_uri(
            dataset,
            version,
            AssetType.raster_tile_set,
            co.dict(by_alias=True),
            "epsg:3857",
        )
    )
    target_prefix = posixpath.dirname(split_s3_path(target_asset_uri)[1])

    command = [
        "apply_colormap.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-j",
        symbology,
        "-n",
        no_data,
        "-s",
        source_asset_uri,
        "-T",
        target_prefix,
    ]

    kwargs = dict()
    if co.timeout_sec is not None:
        kwargs["attempt_duration_seconds"] = co.timeout_sec

    return GDALDEMJob(
        dataset=dataset,
        job_name=job_name,
        command=command,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents] if parents else None,
        **kwargs,
    )


async def create_resample_job(
    dataset: str,
    version: str,
    co: PixETLCreationOptions,
    zoom_level: int,
    job_name: str,
    callback: Callback,
    parents: Optional[List[Job]] = None,
):
    """Create a Batch job to process rasters using the GDAL CLI utilities
    rather than pixetl.

    Suitable only for resampling from (EPSG:4326 or EPSG:3857) to
    EPSG:3857 with no calc string.
    """
    assert isinstance(co.source_uri, List) and len(co.source_uri) == 1
    source_asset_uri = co.source_uri[0]

    if co.calc is not None:
        raise ValueError(
            "Attempting to run the resample script with a calc string specified!"
        )

    target_asset_uri = tile_uri_to_tiles_geojson(
        get_asset_uri(
            dataset,
            version,
            AssetType.raster_tile_set,
            co.dict(by_alias=True),
            "epsg:3857",
        )
    )
    # We want to wind up with "dataset/version/raster/projection/zoom_level/implementation"
    target_prefix = posixpath.dirname(
        posixpath.dirname(split_s3_path(target_asset_uri)[1])
    )

    # The GDAL utilities use "near" whereas rasterio/pixetl use "nearest"
    resampling_method = (
        "near" if co.resampling == ResamplingMethod.nearest else co.resampling
    )

    command = [
        "resample.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-s",
        source_asset_uri,
        "-r",
        f"{resampling_method}",
        "--zoom_level",
        f"{zoom_level}",
        "-T",
        target_prefix,
    ]

    kwargs = dict()
    if co.timeout_sec is not None:
        kwargs["attempt_duration_seconds"] = co.timeout_sec

    return PixETLJob(
        dataset=dataset,
        job_name=job_name,
        command=command,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents] if parents else None,
        **kwargs,
    )
