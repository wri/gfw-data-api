from typing import Any

from .base import StrictBaseModel


class Response(StrictBaseModel):
    data: Any
    status: str = "success"


class PaginationMeta(StrictBaseModel):
    size: int
    total_items: int
    total_pages: int
