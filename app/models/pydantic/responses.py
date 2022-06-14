from typing import Any, Dict

from .base import StrictBaseModel


class Response(StrictBaseModel):
    data: Any
    status: str = "success"


class PaginatedResponse(Response):
    links: Dict
    meta: Dict
