from typing import Any, Dict, List, Optional
from uuid import UUID

from app.crud.assets import get_default_asset
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import (
    RasterTileSetAssetCreationOptions,
    RasterTileSetSourceCreationOptions,
)
from app.tasks import Callback, callback_constructor
from app.tasks.batch import execute
from app.tasks.raster_tile_set_assets.utils import create_pixetl_job


async def raster_tile_set_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:

    # If being created as a source (default) asset, creation_options["source_uri"]
    # will be a list. When being created as an auxiliary asset, it will be None.
    # In the latter case we will generate one for pixETL based on the default asset,
    # below.
    source_uris: Optional[List[str]] = input_data["creation_options"].get("source_uri")
    if source_uris is None:
        creation_options = RasterTileSetAssetCreationOptions(
            **input_data["creation_options"]
        ).dict(exclude_none=True, by_alias=True)

        default_asset = await get_default_asset(dataset, version)

        if default_asset.creation_options["source_type"] == "raster":
            creation_options["source_type"] = "raster"
            creation_options["source_uri"] = default_asset.creation_options[
                "source_uri"
            ]
        elif default_asset.creation_options["source_type"] == "vector":
            creation_options["source_type"] = "vector"
    else:
        # FIXME move to validator function and assess prior to running background task
        if len(source_uris) > 1:
            raise AssertionError("Raster assets currently only support one input file")
        elif len(source_uris) == 0:
            raise AssertionError("source_uri must contain a URI to an input file in S3")
        creation_options = RasterTileSetSourceCreationOptions(
            **input_data["creation_options"]
        ).dict(exclude_none=True, by_alias=True)
        creation_options["source_uri"] = source_uris

    callback: Callback = callback_constructor(asset_id)

    create_raster_tile_set_job = await create_pixetl_job(
        dataset, version, creation_options, "create_raster_tile_set", callback
    )

    log: ChangeLog = await execute([create_raster_tile_set_job])

    return log
