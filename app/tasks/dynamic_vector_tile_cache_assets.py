from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from ..application import ContextEngine
from ..crud import assets
from ..models.enum.assets import AssetStatus, is_database_asset
from ..models.enum.change_log import ChangeLogStatus
from ..models.enum.creation_options import IndexType
from ..models.orm.asset_metadata import FieldMetadata as ORMFieldMetadata
from ..models.orm.assets import Asset as ORMAsset
from ..models.pydantic.change_log import ChangeLog
from ..utils.tile_cache import redeploy_tile_cache_service


async def dynamic_vector_tile_cache_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:
    """Verify given database table asset is present and correctly configured
    for dynamic vector tile cache."""

    async with ContextEngine("READ"):
        orm_assets: List[ORMAsset] = await assets.get_assets_by_filter(dataset, version)

    # Let's first assume that the database table is not correctly configured or present
    # And then try to prove that it is.
    change_log: ChangeLog = ChangeLog(
        date_time=datetime.now(),
        status=ChangeLogStatus.failed,
        message="Failed to Create Dynamic Vector Tile Cache Asset.",
        detail="Associated database table does not meet criteria.",
    )

    # My first walrus, yahoo!
    if orm_asset := _get_database_table_asset(orm_assets):
        fields: List[ORMFieldMetadata] = (
            orm_asset.metadata.fields if orm_asset.metadata else []
        )
        if _has_geom_wm(fields) and _has_spatial_index(orm_asset.creation_options):
            change_log = ChangeLog(
                date_time=datetime.now(),
                status=ChangeLogStatus.success,
                message="Created Dynamic Vector Tile Cache Asset",
            )

            await redeploy_tile_cache_service(asset_id)

    return change_log


def _get_database_table_asset(assets: List[ORMAsset]) -> Optional[ORMAsset]:
    """Fetch database table asset."""
    for asset in assets:
        if is_database_asset(asset.asset_type) and asset.status == AssetStatus.saved:
            return asset
    return None


def _has_geom_wm(fields: List[ORMFieldMetadata]) -> bool:
    """Check if geom_wm column is present."""
    for field in fields:
        if field.name == "geom_wm":
            return True
    return False


def _has_spatial_index(creation_options: Dict[str, Any]) -> bool:
    """Check if geom_wm column has gist index."""
    for index in creation_options["indices"]:
        if index["index_type"] == IndexType.gist and index["column_names"] == [
            "geom_wm"
        ]:
            return True
    return False
