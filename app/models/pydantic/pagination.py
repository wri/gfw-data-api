from pydantic import Field

from app.models.pydantic.base import StrictBaseModel


class PaginationLinks(StrictBaseModel):
    self: str
    first: str
    last: str
    prev: str
    next: str


class PaginationMeta(StrictBaseModel):
    size: int = Field(..., ge=0)
    total_items: int = Field(..., ge=0)
    total_pages: int = Field(..., ge=0)
