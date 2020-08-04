import json
from typing import Any, Dict, List
from uuid import UUID

from fastapi.encoders import jsonable_encoder

from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import RasterSourceCreationOptions
from ..models.pydantic.jobs import PixETLJob
from . import Callback, callback_constructor, writer_secrets
from .batch import execute


async def raster_source_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:

    # pixETL does not currently support combining multiple inputs
    source_uris: List[str] = input_data["creation_options"]["source_uri"]
    if len(source_uris) > 1:
        raise AssertionError("Raster sources only support one input file")
    elif len(source_uris) == 0:
        raise AssertionError("source_uri must contain a URI to an input file in S3")
    source_uri = source_uris[0]

    # Put in a Pydantic model for validation, but then turn into a dict so we
    # can re-define source_uri as a str instead of a List[str] to make pixETL
    # happy
    creation_options = RasterSourceCreationOptions(
        **input_data["creation_options"]
    ).dict()
    creation_options["source_uri"] = source_uri

    from logging import getLogger

    # logger = getLogger("SERIOUSBUSINESS")
    # logger.error(f"CREATION OPTIONS: {jsonable_encoder(creation_options)}")

    callback: Callback = callback_constructor(asset_id)

    job_env = writer_secrets + [{"name": "ASSET_ID", "value": str(asset_id)}]

    # logger.error(f"ENV: {job_env}")

    command = [
        "create_raster_tile_set.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-j",
        json.dumps(jsonable_encoder(creation_options)),
    ]

    if creation_options.get("subset"):
        command += ["--subset", creation_options["subset"]]

    create_raster_tile_set_job = PixETLJob(
        job_name="create_raster_tile_set",
        command=command,
        environment=job_env,
        callback=callback,
    )

    log: ChangeLog = await execute([create_raster_tile_set_job])

    return log
