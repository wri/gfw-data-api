from typing import Any

from app.models.pydantic.base import DataApiBaseModel


class Response(DataApiBaseModel):
    data: Any
    status: str = "success"
