from typing import Any, Dict, List

from app.models.pydantic.change_log import ChangeLog


async def raster_source_asset(
    dataset, version, source_uri: List[str], config_options, metadata: Dict[str, Any],
) -> ChangeLog:
    pass
