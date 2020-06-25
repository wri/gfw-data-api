from typing import Any, Awaitable, Callable, Dict
from uuid import UUID

from ..models.pydantic.change_log import ChangeLog


async def raster_source_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
    callback: Callable[[Dict[str, Any]], Awaitable[None]],
) -> ChangeLog:
    pass
