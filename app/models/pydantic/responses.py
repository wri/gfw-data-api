from typing import Any

from pydantic import BaseModel


class Response(BaseModel):
    data: Any
    status: str = "success"
