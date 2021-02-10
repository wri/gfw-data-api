import copy
import json
from typing import Any, Dict, List, Optional

from fastapi.encoders import jsonable_encoder

from app.models.pydantic.jobs import Job, PixETLJob
from app.settings.globals import (
    AWS_GCS_KEY_SECRET_ARN,
    AWS_REGION,
    ENV,
    S3_ENTRYPOINT_URL,
)
from app.tasks import Callback, writer_secrets

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
    co: Dict[str, Any],
    job_name: str,
    callback: Callback,
    parents: Optional[List[Job]] = None,
) -> Job:
    """Schedule a PixETL Batch Job."""
    co_copy = copy.deepcopy(co)
    if isinstance(co_copy.get("source_uri"), list):
        co_copy["source_uri"] = co_copy["source_uri"][0]
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

    return PixETLJob(
        job_name=job_name,
        command=command,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents] if parents else None,
    )
