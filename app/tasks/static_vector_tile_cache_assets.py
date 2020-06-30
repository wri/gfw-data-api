from typing import Any, Awaitable, Dict, List, Optional
from uuid import UUID

from ..application import ContextEngine
from ..crud import assets, tasks
from ..models.orm.assets import Asset as ORMAsset
from ..models.pydantic.assets import AssetType
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import (
    StaticVectorTileCacheCreationOptions,
    asset_creation_option_factory,
)
from ..models.pydantic.jobs import GdalPythonExportJob, TileCacheJob
from ..models.pydantic.metadata import asset_metadata_factory
from ..settings.globals import DATA_LAKE_BUCKET, TILE_CACHE_JOB_QUEUE
from . import callback_constructor, reader_secrets
from .batch import execute


async def static_vector_tile_cache_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:
    """Create Vector tile cache and NDJSON file as intermediate data."""

    creation_options = asset_creation_option_factory(
        None, AssetType.static_vector_tile_cache, input_data["creation_options"]
    )

    await assets.update_asset(
        asset_id,
        metadata={
            "min_zoom": creation_options.min_zoom,
            "max_zoom": creation_options.max_zoom,
        },
    )

    field_attributes: List[Dict[str, Any]] = await _get_field_attributes(
        dataset, version, creation_options
    )

    if input_data["metadata"] is None:
        _metadata = {}
    else:
        _metadata = input_data["metadata"]
    _metadata["fields_"] = field_attributes

    metadata = asset_metadata_factory(AssetType.ndjson, _metadata)

    ndjson_uri = f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/vector/epsg:4326/{dataset}_{version}.ndjson"

    # We create a NDJSON file as intermediate data and will add it as an asset implicitly.
    # TODO: Will need to list the available fields in metadata. Should be the same as listed for tile cache
    ndjson_asset: ORMAsset = await assets.create_asset(
        dataset,
        version,
        asset_type=AssetType.ndjson,
        asset_uri=ndjson_uri,
        metadata=metadata,
    )

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
        ",".join([field["field_name_"] for field in field_attributes]),
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
    ]

    create_vector_tile_cache = TileCacheJob(
        job_name="create_vector_tile_cache",
        command=command,
        parents=[export_ndjson.job_name],
        callback=callback_constructor(asset_id),
    )

    log: ChangeLog = await execute([export_ndjson, create_vector_tile_cache])

    return log


async def _get_field_attributes(
    dataset: str, version: str, creation_options: StaticVectorTileCacheCreationOptions
) -> List[Dict[str, Any]]:
    """Get field attribute list from creation options.

    If no attribute list provided, use all fields from DB table, marked
    as `is_feature_info`. Otherwise compare to provide list with
    available fields and use intersection.
    """

    orm_assets: List[ORMAsset] = await assets.get_assets(dataset, version)
    fields: Optional[List[Dict[str, str]]] = None
    for asset in orm_assets:
        if asset.is_default:
            fields = asset.metadata["fields_"]
            break

    if fields:
        field_attributes: List[Dict[str, Any]] = [
            field for field in fields if field["is_feature_info"]
        ]
    else:
        raise RuntimeError("No default asset found.")

    if creation_options.field_attributes:
        field_attributes = [
            field
            for field in field_attributes
            if field["field_name_"] in creation_options.field_attributes
        ]

    return field_attributes
