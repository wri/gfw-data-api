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

    # source_uris: List[str] = input_data["creation_options"]["source_uri"]
    # FIXME: What to do with multiple input files? Create a job for each? Should they each be separate assets?
    # Should we disallow more than one? What if there are none?

    creation_options = RasterSourceCreationOptions(**input_data["creation_options"])

    from logging import getLogger

    logger = getLogger("SERIOUSBUSINESS")
    logger.error(f"CREATION OPTIONS: {jsonable_encoder(creation_options)}")

    callback: Callback = callback_constructor(asset_id)

    job_env = writer_secrets + [{"name": "ASSET_ID", "value": str(asset_id)}]

    logger.error(f"ENV: {job_env}")

    # FIXME: Should we enable overwrite categorically? Does it matter? It's only writing to the container, right?
    # FIXME: Don't forget subset!

    create_raster_tile_set_job = PixETLJob(
        job_name="create_raster_tile_set",
        command=[
            "create_raster_tile_set.sh",
            "-d",
            dataset,
            "-v",
            version,
            "-j",
            json.dumps(jsonable_encoder(creation_options)),
        ],
        environment=job_env,
        callback=callback,
    )

    log: ChangeLog = await execute([create_raster_tile_set_job])

    return log
