from datetime import datetime
from uuid import UUID

from botocore.exceptions import ClientError
from fastapi.logger import logger

from ..crud import assets
from ..models.enum.change_log import ChangeLogStatus
from ..models.pydantic.change_log import ChangeLog
from ..settings.globals import TILE_CACHE_CLUSTER, TILE_CACHE_SERVICE
from ..tasks.aws_tasks import update_ecs_service


async def redeploy_tile_cache_service(asset_id: UUID) -> None:
    """Redeploy Tile cache service to make sure dynamic tile cache is
    recognized."""
    try:
        update_ecs_service(TILE_CACHE_CLUSTER, TILE_CACHE_SERVICE)
        ecs_change_log = ChangeLog(
            date_time=datetime.now(),
            status=ChangeLogStatus.success,
            message="Redeployed Tile Cache Service",
        )
    except ClientError as e:
        # Let's don't make this a blocker but make sure it gets logged in case something goes wrong
        logger.exception(str(e))
        ecs_change_log = ChangeLog(
            date_time=datetime.now(),
            status=ChangeLogStatus.failed,
            message="Failed to redeploy Tile Cache Service",
            detail=str(e),
        )
    await assets.update_asset(asset_id, change_log=[ecs_change_log.dict(by_alias=True)])
