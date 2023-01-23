from datetime import datetime
from typing import Any, Callable, Coroutine, Dict, FrozenSet, Union
from uuid import UUID

from ..application import ContextEngine
from ..crud import assets, versions
from ..models.enum.assets import AssetStatus
from ..models.enum.change_log import ChangeLogStatus
from ..models.enum.sources import SourceType
from ..models.pydantic.assets import AssetType
from ..models.pydantic.change_log import ChangeLog
from .dynamic_vector_tile_cache_assets import dynamic_vector_tile_cache_asset
from .raster_tile_cache_assets import raster_tile_cache_asset
from .raster_tile_set_assets import raster_tile_set_asset
from .static_vector_1x1_assets import static_vector_1x1_asset
from .static_vector_file_assets import static_vector_file_asset
from .static_vector_tile_cache_assets import static_vector_tile_cache_asset

Pipeline = Callable[[str, str, UUID, Dict[str, Any]], Coroutine[Any, Any, ChangeLog]]

ASSET_PIPELINES: FrozenSet[AssetType] = frozenset(
    {
        AssetType.shapefile: static_vector_file_asset,
        AssetType.geopackage: static_vector_file_asset,
        # AssetType.ndjson: ndjson_asset,
        # AssetType.csv: csv_asset,
        # AssetType.tsv: tsv_asset,
        AssetType.dynamic_vector_tile_cache: dynamic_vector_tile_cache_asset,
        AssetType.static_vector_tile_cache: static_vector_tile_cache_asset,
        AssetType.grid_1x1: static_vector_1x1_asset,
        # AssetType.vector_tile_cache: vector_tile_cache_asset,
        AssetType.raster_tile_cache: raster_tile_cache_asset,
        # AssetType.dynamic_raster_tile_cache: dynamic_raster_tile_cache_asset,
        AssetType.raster_tile_set: raster_tile_set_asset,
    }.items()
)


async def put_asset(
    asset_type: str,
    asset_id: UUID,
    dataset: str,
    version: str,
    input_data: Dict[str, Any],
    constructor: FrozenSet[Union[AssetType, SourceType]] = ASSET_PIPELINES,
) -> None:
    """Call Asset Pipeline.

    Default assets use source_type for identification. All other assets
    use asset_type directly.
    """
    asset_constructor: Dict[str, Pipeline] = dict(constructor)

    try:

        if asset_type in asset_constructor:
            log: ChangeLog = await asset_constructor[asset_type](
                dataset,
                version,
                asset_id,
                input_data,
            )

        else:
            raise NotImplementedError(f"Unsupported asset type {asset_type}")

    # Make sure asset status is set to `failed` in case there is an uncaught Exception
    except Exception as e:
        change_log = ChangeLog(
            date_time=datetime.now(),
            status=ChangeLogStatus.failed,
            message="Failed to create or update asset. An unexpected error occurred",
            detail=str(e),
        )
        async with ContextEngine("WRITE"):
            await assets.update_asset(
                asset_id,
                status=AssetStatus.failed,
                change_log=[change_log.dict(by_alias=True)],
            )
        raise

    if log.status == ChangeLogStatus.success:
        status = AssetStatus.saved
    else:
        status = log.status

    # Update asset status and change log
    async with ContextEngine("WRITE"):
        await assets.update_asset(
            asset_id, status=status, change_log=[log.dict(by_alias=True)]
        )
        await versions.update_version(
            dataset, version, change_log=[log.dict(by_alias=True)]
        )
