from typing import Dict, Optional, Tuple, Union

from pydantic import Field, StrictInt

from app.models.enum.symbology import ColorMapType
from app.models.pydantic.base import DataApiBaseModel


class RGBA(DataApiBaseModel):
    red: StrictInt = Field(..., ge=0, le=255)
    green: StrictInt = Field(..., ge=0, le=255)
    blue: StrictInt = Field(..., ge=0, le=255)
    alpha: StrictInt = Field(255, ge=0, le=255)

    def tuple(self) -> Tuple[StrictInt, StrictInt, StrictInt, StrictInt]:
        return self.red, self.green, self.blue, self.alpha


class Symbology(DataApiBaseModel):
    type: ColorMapType
    colormap: Optional[Dict[Union[StrictInt, float], RGBA]]
