import io
import json
from typing import Any, Dict, List
from uuid import UUID

from ..crud import assets
from ..models.enum.creation_options import VectorDrivers
from ..models.orm.assets import Asset as ORMAsset
from ..models.pydantic.assets import AssetType
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import (
    StaticVectorTileCacheCreationOptions,
    creation_option_factory,
)
from ..models.pydantic.jobs import GdalPythonExportJob, TileCacheJob
from ..settings.globals import TILE_CACHE_BUCKET, TILE_CACHE_JOB_QUEUE, TILE_CACHE_URL
from ..utils.aws import get_s3_client
from ..utils.fields import get_field_attributes
from ..utils.path import get_asset_uri
from . import callback_constructor, reader_secrets, report_vars
from .batch import execute


async def static_vector_shp_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:
    """Create Vector tile cache and NDJSON file as intermediate data."""

    #######################
    # Update asset metadata
    #######################

    creation_options = creation_option_factory(
        AssetType.shapefile, input_data["creation_options"]
    )

    field_attributes: List[Dict[str, Any]] = await get_field_attributes(
        dataset, version, creation_options
    )

    await assets.update_asset(
        asset_id, fields=field_attributes,
    )

    shp_uri = get_asset_uri(dataset, version, AssetType.shapefile)

    ############################
    # Define jobs
    ############################

    # Export Shapefile
    command: List[str] = [
        "export_vector_data.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-f",
        f"{dataset}_{version}.shp",
        "-F",
        VectorDrivers.shp,
        "-T",
        shp_uri,
        "-C",
        ",".join([field["field_name"] for field in field_attributes]),
        "-X",
        str(True),
    ]

    export_shp = GdalPythonExportJob(
        job_name="export_shp",
        command=command,
        environment=reader_secrets,
        callback=callback_constructor(asset_id),
    )

    #######################
    # execute jobs
    #######################

    log: ChangeLog = await execute([export_shp])

    return log
