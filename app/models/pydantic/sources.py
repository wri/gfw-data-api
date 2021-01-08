from typing import List, Optional

from app.models.pydantic.base import StrictBaseModel


class Source(StrictBaseModel):
    source_uri: Optional[List[str]]
