from enum import Enum
from typing import Dict, Optional, Tuple, Union

from pydantic import BaseModel, Field

from app.models.enum.symbology import ColorMapType


class RGBA(BaseModel):
    red: int = Field(..., ge=0, le=255)
    green: int = Field(..., ge=0, le=255)
    blue: int = Field(..., ge=0, le=255)
    alpha: int = Field(..., ge=0, le=255)

    def tuple(self) -> Tuple[int, int, int, int]:
        return self.red, self.green, self.blue, self.alpha


class Symbology(BaseModel):
    type: ColorMapType
    colormap: Optional[Dict[Union[int, float], RGBA]]

    class Config:
        extra = "forbid"
