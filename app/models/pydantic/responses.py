from typing import Any

from .base import StrictBaseModel


class Response(StrictBaseModel):
    data: Any
    status: str = "success"
