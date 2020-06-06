from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Optional
from typing.io import IO

from ..application import ContextEngine
from ..crud import versions
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.sources import SourceType
from ..utils.aws import get_s3_client
from ..utils.path import split_s3_path
from .raster_source_assets import raster_source_asset
from .table_source_assets import table_source_asset
from .vector_source_assets import vector_source_asset

default_asset = {
    SourceType.vector: vector_source_asset,
    SourceType.table: table_source_asset,
    SourceType.raster: raster_source_asset,
}


async def create_default_asset(
    dataset: str,
    version: str,
    input_data: Dict[str, Any],
    file_obj: Optional[IO],
    callback: Callable[[Dict[str, Any]], Awaitable[None]],
) -> None:

    source_type = input_data["source_type"]
    source_uri = input_data["source_uri"]
    creation_options = input_data["creation_options"]
    metadata = input_data["metadata"]

    status = None

    # Copy attached file to data lake
    if file_obj:
        log: ChangeLog = await _inject_file(file_obj, source_uri[0])
        async with ContextEngine("PUT"):
            await versions.update_version(dataset, version, change_log=[log.dict()])
        status = log.status

    if status != "failed":
        # Seed default asset and create asset record in database
        if source_type in default_asset.keys():
            log = await default_asset[source_type](
                dataset, version, source_uri, creation_options, metadata, callback
            )
            status = log.status
        else:
            raise NotImplementedError(f"Unsupported asset source type {source_type})")

    # Update version status and change log
    async with ContextEngine("PUT"):
        await versions.update_version(
            dataset, version, status=status, change_log=[log.dict()]
        )


async def _inject_file(file_obj: IO, s3_uri: str) -> ChangeLog:
    """ Upload a file-like object to S3 data lake """

    s3 = get_s3_client()
    bucket, path = split_s3_path(s3_uri)

    try:
        s3.upload_fileobj(file_obj, bucket, path)
        status = "success"
        message = f"Injected file {path} into {bucket}"
        detail = None
    except Exception as e:
        status = "failed"
        message = f"Failed to injected file {path} into {bucket}"
        detail = str(e)

    return ChangeLog(
        date_time=datetime.now(), status=status, message=message, detail=detail
    )
