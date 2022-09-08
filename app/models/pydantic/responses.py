from typing import Any, Optional

from pydantic import Field

from .base import StrictBaseModel


class Response(StrictBaseModel):
    data: Any
    status: str = "success"


class PaginationLinks(StrictBaseModel):
    self: str = Field(
        ...,
        title="Contains the URL for the current page",
        example="https://data-api.globalforestwatch.org/:model?page[number]=1&page[size]=25",
    )
    first: str = Field(
        ...,
        title="Contains the URL for the first page",
        example="https://data-api.globalforestwatch.org/:model?page[number]=1&page[size]=25",
    )
    last: str = Field(
        ...,
        title="Contains the URL for the last page",
        example="https://data-api.globalforestwatch.org/:model?page[number]=4&page[size]=25",
    )
    prev: Optional[str] = Field(
        None, title="Contains the URL for the previous page", example=""
    )
    next: Optional[str] = Field(
        None,
        title="Contains the URL for the next page",
        example="https://data-api.globalforestwatch.org/:model?page[number]=2&page[size]=25",
    )


class PaginationMeta(StrictBaseModel):
    size: int = Field(
        ...,
        title="The page size. Reflects the value used in the page[size] query parameter (or the default size of 10 if not provided)",
        example="25",
    )
    total_items: int = Field(
        ...,
        title="Contains the total number of items",
        example="100",
    )
    total_pages: int = Field(
        ...,
        title="Contains the total number of pages, assuming the page size specified in the page[size] query parameter",
        example="4",
    )
