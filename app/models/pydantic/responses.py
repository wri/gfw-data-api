from typing import Any

from .base import StrictBaseModel


class Response(StrictBaseModel):
    data: Any
    status: str = "success"


class PaginationLinks(StrictBaseModel):
    self: str
    first: str
    last: str
    prev: str
    next: str


class PaginationMeta(StrictBaseModel):
    size: int
    total_items: int
    total_pages: int
