from typing import Any, Dict, FrozenSet, Tuple
from uuid import UUID

from ..application import ContextEngine
from ..crud import assets, versions
from ..models.pydantic.change_log import ChangeLog

ASSET_PIPELINES: FrozenSet[Tuple[Any, Any]] = frozenset(
    {
        # AssetType.shapefile: shapefile_asset,
        # AssetType.geopackage: geopackage_asset,
        # AssetType.ndjson: ndjson_asset,
        # AssetType.csv: csv_asset,
        # AssetType.tsv: tsv_asset,
        # AssetType.dynamic_vector_tile_cache: dynamic_vector_tile_cache_asset,
        # AssetType.vector_tile_cache: vector_tile_cache_asset,
        # AssetType.raster_tile_cache: raster_tile_cache_asset,
        # AssetType.dynamic_raster_tile_cache: dynamic_raster_tile_cache_asset,
        # AssetType.raster_tile_set: raster_tile_set_asset
    }.items()
)


async def create_asset(
    asset_type: str,
    asset_id: UUID,
    dataset: str,
    version: str,
    input_data: Dict[str, Any],
    asset_lookup: FrozenSet[Tuple[Any, Any]] = ASSET_PIPELINES,
) -> None:
    """Call Asset Pipeline.

    Default assets use source_type for identification. All other assets
    use asset_type directly.
    """
    lookup = dict(asset_lookup)

    try:

        if asset_type in lookup.keys():
            log: ChangeLog = await lookup[asset_type](
                dataset, version, asset_id, input_data,
            )

        else:
            raise NotImplementedError(f"Unsupported asset type {asset_type}")

    # Make sure asset status is set to `failed` in case there is an uncaught Exception
    except Exception:
        async with ContextEngine("WRITE"):
            await assets.update_asset(asset_id, status="failed")
        raise

    # Update version status and change log
    async with ContextEngine("WRITE"):
        await versions.update_version(
            dataset, version, status=log.status, change_log=[log.dict()]
        )


async def vector_tile_cache_asset():
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


async def raster_tile_cache_asset():
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


async def dynamic_raster_tile_cache_asset():
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


async def raster_tile_set_asset():
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
