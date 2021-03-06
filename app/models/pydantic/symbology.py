from typing import Dict, Optional, Tuple, Union

from pydantic import Field, StrictInt

from app.models.enum.symbology import ColorMapType
from app.models.pydantic.base import StrictBaseModel


class RGBA(StrictBaseModel):
    red: int = Field(..., ge=0, le=255)
    green: int = Field(..., ge=0, le=255)
    blue: int = Field(..., ge=0, le=255)
    alpha: int = Field(255, ge=0, le=255)

    def tuple(self) -> Tuple[int, int, int, int]:
        return self.red, self.green, self.blue, self.alpha


class Symbology(StrictBaseModel):
    type: ColorMapType
    colormap: Optional[Dict[Union[StrictInt, float], RGBA]]
