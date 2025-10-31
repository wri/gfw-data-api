from typing import List, Optional

from .base import StrictBaseModel


class Source(StrictBaseModel):
    source_uri: Optional[List[str]]
