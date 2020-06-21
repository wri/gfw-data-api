from datetime import datetime
from typing import Any, Dict, Optional
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
    dataset: str, version: str, input_data: Dict[str, Any], file_obj: Optional[IO],
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
                dataset, version, source_uri, creation_options, metadata
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
        message = f"Failed to inject file {path} into {bucket}"
        detail = str(e)

    return ChangeLog(
        date_time=datetime.now(), status=status, message=message, detail=detail
    )


async def _create_static_vector_tile_cache():
    # supported input types
    #  - vector

    # steps
    #  - wait until database table is created
    #  - export ndjson file
    #  - generate static vector tiles using tippecanoe and upload to S3
    #  - create static vector tile asset entry to enable service

    # creation options:
    #  - default symbology/ legend
    #  - tiling strategy
    #  - min/max zoom level
    #  - caching strategy

    # custom metadata
    #  - default symbology/ legend
    #  - rendered zoom levels

    raise NotImplementedError


async def _create_static_raster_tile_cache():
    # supported input types
    #  - raster
    #  - vector ?

    # steps
    # create raster tile cache using mapnik and upload to S3
    # register static raster tile cache asset entry to enable service

    # creation options:
    #  - symbology/ legend
    #  - tiling strategy
    #  - min/max zoom level
    #  - caching strategy

    # custom metadata
    #  - symbology/ legend
    #  - rendered zoom levels

    raise NotImplementedError


async def _create_dynamic_raster_tile_cache():
    # supported input types
    #  - raster
    #  - vector ?

    # steps
    # create raster set (pixETL) using WebMercator grid
    # register dynamic raster tile cache asset entry to enable service

    # creation options:
    #  - symbology/ legend
    #  - tiling strategy
    #  - min/max zoom level
    #  - caching strategy

    # custom metadata
    #  - symbology/ legend

    raise NotImplementedError


async def _create_tile_set():
    # supported input types
    #  - vector
    #  - raster

    # steps
    #  - wait until database table is created (vector only)
    #  - create 1x1 materialized view (vector only)
    #  - create raster tiles using pixETL and upload to S3
    #  - create tile set asset entry

    # creation options
    #  - set tile set value name
    #  - select field value or expression to use for rasterization (vector only)
    #  - select order direction (asc/desc) of field values for rasterization (vector only)
    #  - override input raster, must be another raster tile set of the same version (raster only)
    #  - define numpy calc expression (raster only)
    #  - select resampling method (raster only)
    #  - select out raster datatype
    #  - select out raster nbit value
    #  - select out raster no data value
    #  - select out raster grid type

    # custom metadata
    #  - raster statistics
    #  - raster table (pixel value look up)
    #  - list of raster files
    #  - raster data type
    #  - compression
    #  - no data value

    raise NotImplementedError
