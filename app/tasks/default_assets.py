from datetime import datetime
from typing import Any, Dict, FrozenSet, Optional
from typing.io import IO
from uuid import UUID

from ..application import ContextEngine
from ..crud import assets, versions
from ..models.enum.assets import default_asset_type
from ..models.enum.change_log import ChangeLogStatus
from ..models.enum.sources import SourceType
from ..models.pydantic.asset_metadata import asset_metadata_factory
from ..models.pydantic.assets import AssetTaskCreate
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import creation_option_factory
from ..utils.aws import get_s3_client
from ..utils.path import get_asset_uri, split_s3_path
from .assets import put_asset
from .raster_tile_set_assets import raster_tile_set_asset
from .table_source_assets import append_table_source_asset, table_source_asset
from .vector_source_assets import append_vector_source_asset, vector_source_asset

# WRT type, these look more to me like
# FrozenSet[Tuple[SourceType, Coroutine[Any, Any, ChangeLog]]
# but I cannot get PyCharm to agree.
DEFAULT_ASSET_PIPELINES: FrozenSet[SourceType] = frozenset(
    {
        SourceType.vector: vector_source_asset,
        SourceType.table: table_source_asset,
        SourceType.raster: raster_tile_set_asset,
    }.items()
)
DEFAULT_APPEND_ASSET_PIPELINES: FrozenSet[SourceType] = frozenset(
    {
        SourceType.table: append_table_source_asset,
        SourceType.vector: append_vector_source_asset,
    }.items()
)


async def create_default_asset(
    dataset: str,
    version: str,
    input_data: Dict[str, Any],
    file_obj: Optional[IO],
) -> UUID:

    source_uri = input_data["creation_options"]["source_uri"]
    status = None
    log: Optional[ChangeLog] = None

    # Copy attached file to data lake
    if file_obj:
        log = await _inject_file(file_obj, source_uri[0])
        async with ContextEngine("WRITE"):
            await versions.update_version(
                dataset, version, change_log=[log.dict(by_alias=True)]
            )

    if log and log.status == "failed":
        # Update version status and change log
        async with ContextEngine("WRITE"):
            await versions.update_version(
                dataset, version, status=status, change_log=[log.dict(by_alias=True)]
            )
        raise RuntimeError(f"Could not create asset for {dataset}.{version}")

    # register asset and start the pipeline
    else:
        try:
            asset_id: UUID = await _create_default_asset(
                dataset=dataset,
                version=version,
                input_data=input_data,
            )
            return asset_id
        # Make sure version status is set to `failed` in case there is an uncaught Exception

        except Exception:
            async with ContextEngine("WRITE"):
                await versions.update_version(dataset, version, status="failed")
            raise


async def append_default_asset(
    dataset: str, version: str, input_data: Dict[str, Any], asset_id: UUID
) -> None:
    source_type = input_data["creation_options"]["source_type"]

    try:
        await put_asset(
            source_type,
            asset_id=asset_id,
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
    dataset: str,
    version: str,
    input_data: Dict[str, Any],
) -> UUID:
    creation_option = input_data["creation_options"]
    source_type = creation_option["source_type"]
    asset_type = default_asset_type(source_type, creation_option)
    asset_uri = get_asset_uri(dataset, version, asset_type, creation_option)
    creation_options = creation_option_factory(asset_type, creation_option)

    data = AssetTaskCreate(
        asset_type=asset_type,
        dataset=dataset,
        version=version,
        asset_uri=asset_uri,
        is_managed=True,
        is_default=True,
        creation_options=creation_options,
    )

    async with ContextEngine("WRITE"):
        new_asset = await assets.create_asset(**data.dict(by_alias=True))

    await put_asset(
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
