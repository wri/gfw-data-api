from typing import Dict, Optional, Tuple, Union

from pydantic import Field, StrictInt, validator

from app.models.enum.creation_options import ColorMapType
from app.models.pydantic.base import StrictBaseModel


class RGB(StrictBaseModel):
    red: int = Field(..., ge=0, le=255)
    green: int = Field(..., ge=0, le=255)
    blue: int = Field(..., ge=0, le=255)

    def tuple(self) -> Tuple[int, int, int]:
        return self.red, self.green, self.blue


class RGBA(StrictBaseModel):
    red: int = Field(..., ge=0, le=255)
    green: int = Field(..., ge=0, le=255)
    blue: int = Field(..., ge=0, le=255)
    alpha: int = Field(..., ge=0, le=255)

    def tuple(self) -> Tuple[int, int, int, int]:
        return self.red, self.green, self.blue, self.alpha


class Symbology(StrictBaseModel):
    type: ColorMapType
    colormap: Optional[Dict[Union[StrictInt, float], Union[RGB, RGBA]]]

    @validator("colormap")
    def colormap_alpha_val(cls, v, values):
        if v is not None:
            break_points = [value for key, value in v.items()]
            if "type" in values and values["type"] in (
                ColorMapType.discrete_intensity,
                ColorMapType.gradient_intensity,
            ):
                assert all(
                    isinstance(value, RGB) for value in break_points
                ), "Breakpoints for intensity colormaps must not include alpha values"
            assert (
                len(set([type(value) for value in break_points])) == 1
            ), "Colormap breakpoints must be either all RGB or all RGBA"
        return v
