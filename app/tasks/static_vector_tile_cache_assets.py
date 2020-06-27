from typing import Any, Awaitable, Dict, List, Optional
from uuid import UUID

from ..application import ContextEngine
from ..crud import assets, tasks
from ..models.orm.assets import Asset as ORMAsset
from ..models.pydantic.assets import AssetType
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import StaticVectorTileCacheCreationOptions
from ..models.pydantic.jobs import GdalPythonExportJob, TileCacheJob
from ..settings.globals import DATA_LAKE_BUCKET, TILE_CACHE_JOB_QUEUE
from . import reader_secrets, update_asset_status
from .batch import execute


async def static_vector_tile_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:
    """Create Vector tile cache and NDJSON file as intermediate data."""

    async def callback(
        task_id: Optional[UUID], message: Dict[str, Any]
    ) -> Awaitable[None]:
        async with ContextEngine("PUT"):
            if task_id:
                _ = await tasks.create_task(
                    task_id, asset_id=asset_id, change_log=[message]
                )
            return await assets.update_asset(asset_id, change_log=[message])

    creation_options = StaticVectorTileCacheCreationOptions(
        **input_data["creation_options"]
    )

    field_attributes: List[str] = await _get_field_attributes(
        dataset, version, creation_options
    )

    ndjson_uri = f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/vector/epsg:4326/{dataset}_{version}.ndjson"

    # We create a NDJSON file as intermediate data and will add it as an asset implicitly.
    # TODO: Will need to list the available fields in metadata. Should be the same as listed for tile cache
    ndjson_asset: ORMAsset = await assets.create_asset(
        dataset, version, asset_type=AssetType.ndjson, asset_uri=ndjson_uri,
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
        ",".join(field_attributes),
    ]

    export_ndjson = GdalPythonExportJob(
        job_name="export_ndjson",
        job_queue=TILE_CACHE_JOB_QUEUE,
        command=command,
        environment=reader_secrets,
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
    )

    log: ChangeLog = await execute(
        [export_ndjson, create_vector_tile_cache], callback,
    )

    # TODO: this will change once using the TASK route approach
    #  here we will need to associate each job with a different asset
    #  export ndjson with NDJSON asset, create tile cache with Static vector tile cache
    await update_asset_status(asset_id, log.status)
    await update_asset_status(ndjson_asset.asset_id, log.status)

    return log


async def _get_field_attributes(
    dataset: str, version: str, creation_options: StaticVectorTileCacheCreationOptions
) -> List[str]:
    """Get field attribute list from creation options.

    If no attribute list provided, use fields from DB table, marked as
    `is_feature_info`
    """

    if creation_options.field_attributes:
        field_attributes: List[str] = creation_options.field_attributes
    else:
        orm_assets: List[ORMAsset] = await assets.get_assets(dataset, version)
        fields: Optional[List[Dict[str, str]]] = None
        for asset in orm_assets:
            if asset.is_default:
                fields = asset.metadata["fields_"]
                break
        if fields:
            field_attributes = [
                field["field_name_"] for field in fields if field["is_feature_info"]
            ]
        else:
            raise RuntimeError("No default asset found.")

    return field_attributes
