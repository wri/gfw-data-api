from typing import List, Dict, Any

from app.models.pydantic.change_log import ChangeLog


async def raster_source_asset(
    dataset,
    version,
    source_uri: List[str],
    config_options,
    metadata: Dict[str, Any],
    callback,
) -> ChangeLog:
    pass
