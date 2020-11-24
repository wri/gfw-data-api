from typing import Dict, Union

from pydantic import BaseModel


class DistinctColorMap(BaseModel):
    type = "distinct"
    map: Dict  # Speculative, fill in later


class DateConfIntensityColorMap(BaseModel):
    type = "date_conf_intensity"


class GradientColorMap(BaseModel):
    type = "gradient"
    map: Dict  # Speculative, fill in later


ColorMapType = Union[DateConfIntensityColorMap, DistinctColorMap, GradientColorMap]


class Symbology(BaseModel):
    color_map: ColorMapType
