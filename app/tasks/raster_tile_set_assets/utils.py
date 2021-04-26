import json
import posixpath
from typing import List, Optional

from fastapi.encoders import jsonable_encoder

from app.models.enum.assets import AssetType
from app.models.pydantic.creation_options import PixETLCreationOptions
from app.models.pydantic.jobs import GDALDEMJob, Job, PixETLJob
from app.settings.globals import (
    AWS_GCS_KEY_SECRET_ARN,
    AWS_REGION,
    DEFAULT_JOB_DURATION,
    ENV,
    MAX_CORES,
    MAX_MEM,
    S3_ENTRYPOINT_URL,
)
from app.tasks import Callback, writer_secrets
from app.utils.path import get_asset_uri, split_s3_path, tile_uri_to_tiles_geojson

JOB_ENV = writer_secrets + [
    {"name": "AWS_REGION", "value": AWS_REGION},
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
    """Schedule a PixETL Batch Job."""
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

    # allowing float jobs to run longer
    if "float" in co.data_type:
        kwargs = {
            "attempt_duration_seconds": int(DEFAULT_JOB_DURATION * 3),
            "vcpus": MAX_CORES,
            "memory": MAX_MEM,
        }
    else:
        kwargs = {}

    return PixETLJob(
        dataset=dataset,
        job_name=job_name,
        command=command,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents] if parents else None,
        **kwargs
    )


async def create_gdaldem_job(
    dataset: str,
    version: str,
    co: PixETLCreationOptions,
    job_name: str,
    callback: Callback,
    parents: Optional[List[Job]] = None,
):
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
        "apply_symbology.sh",
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

    return GDALDEMJob(
        dataset=dataset,
        job_name=job_name,
        command=command,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents] if parents else None,
    )
