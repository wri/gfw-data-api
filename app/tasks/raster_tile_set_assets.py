import json
from typing import Any, Dict, List
from uuid import UUID

from fastapi.encoders import jsonable_encoder

from app.crud.assets import get_default_asset
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import RasterTileSetAssetCreationOptions
from app.models.pydantic.jobs import PixETLJob
from app.settings.globals import ENV, S3_ENTRYPOINT_URL
from app.tasks import Callback, callback_constructor, writer_secrets
from app.tasks.batch import execute


async def raster_tile_set_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:

    # When being created as an auxiliary asset, creation_options["source_uri"] should
    # never be populated. We will generate one for pixETL based on the default asset,
    # below.
    source_uris: List[str] = input_data["creation_options"].get("source_uri")
    if source_uris is not None:
        raise AssertionError(
            "Specifying a source_uri when adding auxiliary raster tile set assets is currently unsupported."
        )

    creation_options = RasterTileSetAssetCreationOptions(
        **input_data["creation_options"]
    ).dict()

    default_asset = await get_default_asset(dataset, version)

    if default_asset.creation_options["source_type"] == "raster":
        creation_options["source_type"] = "raster"
        creation_options["source_uri"] = default_asset.creation_options["source_uri"]
    elif default_asset.creation_options["source_type"] == "vector":
        creation_options["source_type"] = "vector"
    else:
        raise AssertionError(
            f"Default asset has source_type of {default_asset.creation_options['source_type']}"
        )

    overwrite = creation_options.pop("overwrite")
    subset = creation_options.pop("subset")
    layer_def = json.dumps(jsonable_encoder(creation_options))

    callback: Callback = callback_constructor(asset_id)

    job_env = writer_secrets + [
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
