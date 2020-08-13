from typing import Any, Dict, List
from uuid import UUID

from ..crud import assets
from ..models.orm.assets import Asset as ORMAsset
from ..models.pydantic.assets import AssetType
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import creation_option_factory
from ..models.pydantic.jobs import GdalPythonExportJob, TileCacheJob
from ..settings.globals import TILE_CACHE_JOB_QUEUE
from ..utils.fields import get_field_attributes
from ..utils.path import get_asset_uri
from . import callback_constructor, reader_secrets, report_vars
from .batch import execute


async def static_vector_tile_cache_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:
    """Create Vector tile cache and NDJSON file as intermediate data."""

    #######################
    # Update asset metadata
    #######################

    creation_options = creation_option_factory(
        AssetType.static_vector_tile_cache, input_data["creation_options"]
    )

    field_attributes: List[Dict[str, Any]] = await get_field_attributes(
        dataset, version, creation_options
    )

    await assets.update_asset(
        asset_id,
        metadata={
            "min_zoom": creation_options.min_zoom,
            "max_zoom": creation_options.max_zoom,
        },
        fields=field_attributes,
    )

    ############################
    # Create NDJSON asset as side effect
    ############################

    ndjson_uri = get_asset_uri(dataset, version, AssetType.ndjson)

    ndjson_asset: ORMAsset = await assets.create_asset(
        dataset,
        version,
        asset_type=AssetType.ndjson,
        asset_uri=ndjson_uri,
        fields=field_attributes,
    )

    ############################
    # Define jobs
    ############################

    # Create table schema
    command: List[str] = [
        "export_vector_data.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-f",
        f"{dataset}_{version}.ndjson",
        "-F",
        "GeoJSONSeq",
        "-T",
        ndjson_uri,
        "-C",
        ",".join([field["field_name"] for field in field_attributes]),
    ]

    export_ndjson = GdalPythonExportJob(
        job_name="export_ndjson",
        job_queue=TILE_CACHE_JOB_QUEUE,
        command=command,
        environment=reader_secrets,
        callback=callback_constructor(ndjson_asset.asset_id),
    )

    command = [
        "create_vector_tile_cache.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-s",
        ndjson_uri,
        "-Z",
        str(creation_options.min_zoom),
        "-z",
        str(creation_options.max_zoom),
        "-t",
        creation_options.tile_strategy,
        "-I",
        creation_options.implementation,
    ]

    create_vector_tile_cache = TileCacheJob(
        job_name="create_vector_tile_cache",
        command=command,
        parents=[export_ndjson.job_name],
        environment=report_vars,
        callback=callback_constructor(asset_id),
    )

    #######################
    # execute jobs
    #######################

    log: ChangeLog = await execute([export_ndjson, create_vector_tile_cache])

    return log
