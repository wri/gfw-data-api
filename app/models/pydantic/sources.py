from typing import List, Optional

from app.models.pydantic.base import DataApiBaseModel


class Source(DataApiBaseModel):
    source_uri: Optional[List[str]]
