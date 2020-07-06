from datetime import datetime
from typing import Any, Dict, FrozenSet, Optional
from typing.io import IO
from uuid import UUID

from ..application import ContextEngine
from ..crud import assets, versions
from ..models.enum.assets import default_asset_type
from ..models.enum.change_log import ChangeLogStatus
from ..models.enum.sources import SourceType
from ..models.pydantic.assets import AssetTaskCreate
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import asset_creation_option_factory
from ..models.pydantic.metadata import asset_metadata_factory
from ..utils.aws import get_s3_client
from ..utils.path import split_s3_path
from .assets import create_asset
from .raster_source_assets import raster_source_asset
from .table_source_assets import table_source_asset, append_table_source_asset
from .vector_source_assets import vector_source_asset

DEFAULT_ASSET_PIPELINES: FrozenSet[SourceType] = frozenset(
    {
        SourceType.vector: vector_source_asset,
        SourceType.table: table_source_asset,
        SourceType.raster: raster_source_asset,
    }.items()
)

DEFAULT_APPEND_ASSET_PIPELINES: FrozenSet[SourceType] = frozenset(
    {
        SourceType.table: append_table_source_asset,
    }.items()
)

async def create_default_asset(
    dataset: str, version: str, input_data: Dict[str, Any], file_obj: Optional[IO],
) -> UUID:
    source_type = input_data["source_type"]
    source_uri = input_data["source_uri"]
    status = None
    log: Optional[ChangeLog] = None

    # Copy attached file to data lake
    if file_obj:
        log = await _inject_file(file_obj, source_uri[0])
        async with ContextEngine("WRITE"):
            await versions.update_version(dataset, version, change_log=[log.dict()])

    if log and log.status == "failed":
        # Update version status and change log
        async with ContextEngine("WRITE"):
            await versions.update_version(
                dataset, version, status=status, change_log=[log.dict()]
            )
        raise RuntimeError(f"Could not create asset for {dataset}.{version}")

    # register asset and start the pipeline
    else:
        try:
            asset_id: UUID = await _create_default_asset(
                source_type, dataset=dataset, version=version, input_data=input_data,
            )
            return asset_id
        # Make sure version status is set to `failed` in case there is an uncaught Exception

        except Exception:
            async with ContextEngine("WRITE"):
                await versions.update_version(dataset, version, status="failed")
            raise


async def append_default_asset(
    dataset: str, version: str, input_data: Dict[str, Any], file_obj: Optional[IO],
) -> UUID:
    source_type = input_data["source_type"]
    source_uri = input_data["source_uri"]
    try:
        await create_asset(
            source_type,
            asset.asset_id,
            dataset=dataset,
            version=version,
            input_data=input_data,
            constructor=DEFAULT_APPEND_ASSET_PIPELINES,
        )

    # Make sure version status is set to `failed` in case there is an uncaught Exception

    except Exception:
        async with ContextEngine("WRITE"):
            await versions.update_version(dataset, version, status="failed")
        raise


async def _create_default_asset(
    source_type: str, dataset: str, version: str, input_data: Dict[str, Any],
) -> UUID:
    asset_type = default_asset_type(source_type)
    metadata = asset_metadata_factory(asset_type, input_data["metadata"])
    asset_uri = _default_asset_uri(
        source_type, dataset, version, input_data["creation_options"]
    )
    creation_options = asset_creation_option_factory(
        source_type, asset_type, input_data["creation_options"]
    )

    data = AssetTaskCreate(
        asset_type=asset_type,
        dataset=dataset,
        version=version,
        asset_uri=asset_uri,
        is_managed=True,
        is_default=True,
        creation_options=creation_options,
        metadata=metadata,
    )

    async with ContextEngine("WRITE"):
        new_asset = await assets.create_asset(**data.dict())

    await create_asset(
        source_type,
        new_asset.asset_id,
        dataset=dataset,
        version=version,
        input_data=input_data,
        constructor=DEFAULT_ASSET_PIPELINES,
    )

    return new_asset.asset_id


async def _inject_file(file_obj: IO, s3_uri: str) -> ChangeLog:
    """Upload a file-like object to S3 data lake."""

    s3 = get_s3_client()
    bucket, path = split_s3_path(s3_uri)

    try:
        s3.upload_fileobj(file_obj, bucket, path)
        status = ChangeLogStatus.success
        message = f"Injected file {path} into {bucket}"
        detail = None
    except Exception as e:
        status = ChangeLogStatus.failed
        message = f"Failed to inject file {path} into {bucket}"
        detail = str(e)

    return ChangeLog(
        date_time=datetime.now(), status=status, message=message, detail=detail
    )


def _default_asset_uri(source_type, dataset, version, creation_option=None):
    if source_type == "table" or source_type == "vector":
        asset_uri = f"/{dataset}/{version}/features"
    # elif source_type == "raster":
    #     srid = creation_option[]
    #     asset_uri = f"s3://{DATA_LAKE_BUCKET_NAME}/{dataset}/{version}/raster/{srid}/{size}/{col}/{value}/geotiff/{tile_id}.tif"
    else:
        raise NotImplementedError("Not a supported default input source")

    return asset_uri
