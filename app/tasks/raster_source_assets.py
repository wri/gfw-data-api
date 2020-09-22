import json
from typing import Any, Dict, List
from uuid import UUID

from fastapi.encoders import jsonable_encoder

from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from ..models.pydantic.jobs import PixETLJob
from ..settings.globals import ENV, S3_ENTRYPOINT_URL
from . import Callback, callback_constructor, reader_secrets
from .batch import execute


async def raster_source_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:

    # pixETL does not currently support combining multiple inputs
    source_uris: List[str] = input_data["creation_options"].get("source_uri", [])
    if len(source_uris) > 1:
        raise AssertionError("Raster sources only support one input file")
    elif len(source_uris) == 0:
        raise AssertionError("source_uri must contain a URI to an input file in S3")
    source_uri = source_uris[0]

    # Put in a Pydantic model for validation, but then turn into a dict so we
    # can re-define source_uri as a str instead of a List[str] to make pixETL
    # happy
    creation_options = RasterTileSetSourceCreationOptions(
        **input_data["creation_options"]
    ).dict()
    creation_options["source_uri"] = source_uri
    overwrite = creation_options.pop("overwrite")
    subset = creation_options.pop("subset")
    layer_def = json.dumps(jsonable_encoder(creation_options))

    callback: Callback = callback_constructor(asset_id)

    job_env = reader_secrets + [
        {"name": "ENV", "value": ENV},
        {"name": "AWS_S3_ENDPOINT", "value": S3_ENTRYPOINT_URL},
    ]

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
        job_name="create_raster_tile_set",
        command=command,
        environment=job_env,
        callback=callback,
    )

    log: ChangeLog = await execute([create_raster_tile_set_job])

    return log
