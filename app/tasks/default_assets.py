from datetime import datetime
from typing import Any, Dict, Optional
from typing.io import IO

from ..application import ContextEngine
from ..crud import assets, versions
from ..models.pydantic.assets import AssetTaskCreate
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import (
    TableSourceCreationOptions,
    VectorSourceCreationOptions,
)
from ..models.pydantic.metadata import DatabaseTableMetadata
from ..models.pydantic.sources import SourceType
from ..utils.aws import get_s3_client
from ..utils.path import split_s3_path
from .assets import create_asset
from .raster_source_assets import raster_source_asset
from .table_source_assets import table_source_asset
from .vector_source_assets import vector_source_asset

DEFAULT_ASSET_PIPELINES = frozenset(
    {
        SourceType.vector: vector_source_asset,
        SourceType.table: table_source_asset,
        SourceType.raster: raster_source_asset,
    }.items()
)


async def create_default_asset(
    dataset: str, version: str, input_data: Dict[str, Any], file_obj: Optional[IO],
) -> None:
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

    # register asset and start the pipeline
    else:
        try:
            await _create_default_asset(
                source_type, dataset=dataset, version=version, input_data=input_data,
            )
        # Make sure version status is set to `failed` in case there is an uncaught Exception
        except Exception:
            async with ContextEngine("WRITE"):
                await versions.update_version(dataset, version, status="failed")
            raise


async def _create_default_asset(
    source_type: str, dataset: str, version: str, input_data: Dict[str, Any],
):
    asset_type = _default_asset_type(source_type)
    metadata = _default_asset_metadata(source_type, input_data["metadata"])
    asset_uri = _default_asset_uri(
        source_type, dataset, version, input_data["creation_options"]
    )
    creation_options = _default_asset_creation_options(
        source_type, input_data["creation_options"]
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

    return await create_asset(
        source_type,
        new_asset.asset_id,
        dataset=dataset,
        version=version,
        input_data=input_data,
        asset_lookup=DEFAULT_ASSET_PIPELINES,
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
        message = f"Failed to inject file {path} into {bucket}"
        detail = str(e)

    return ChangeLog(
        date_time=datetime.now(), status=status, message=message, detail=detail
    )


def _default_asset_type(source_type):
    if source_type == "table" or source_type == "vector":
        asset_type = "Database table"
    elif source_type == "raster":
        asset_type = "Raster tileset"
    else:
        raise NotImplementedError("Not a supported input source")
    return asset_type


def _default_asset_creation_options(source_type, creation_options):
    if source_type == "vector":
        co = VectorSourceCreationOptions(**creation_options)
    elif source_type == "table":
        co = TableSourceCreationOptions(**creation_options)
    # elif source_type == "raster":
    #     co = RasterSourceCreationOptions(**creation_options)
    else:
        raise NotImplementedError("Not a supported input source")

    return co


def _default_asset_uri(source_type, dataset, version, creation_option=None):
    if source_type == "table" or source_type == "vector":
        asset_uri = f"/{dataset}/{version}/features"
    # elif source_type == "raster":
    #     srid = creation_option[]
    #     asset_uri = f"s3://{DATA_LAKE_BUCKET_NAME}/{dataset}/{version}/raster/{srid}/{size}/{col}/{value}/geotiff/{tile_id}.tif"
    else:
        raise NotImplementedError("Not a supported default input source")

    return asset_uri


def _default_asset_metadata(source_type, metadata):
    if source_type == "table" or source_type == "vector":
        md = DatabaseTableMetadata(**metadata)
    # elif source_type == "raster":
    #     md = RasterSetMetadata(**metadata)
    else:
        raise NotImplementedError("Not a supported default input source")

    return md
