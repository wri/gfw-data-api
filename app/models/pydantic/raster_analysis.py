from typing import List, Optional, Union

from pydantic import BaseModel

from ..enum.pixetl import Grid
from .metadata import RasterTable


class BaseLayer(BaseModel):
    name: str


class EncodedLayer(BaseLayer):
    raster_table: Optional[RasterTable] = None
    decode_expression: str = ""
    encode_expression: str = ""


class SourceLayer(EncodedLayer):
    source_uri: str
    grid: Grid
    tile_scheme: str = "nw"


class DerivedLayer(EncodedLayer):
    source_layer: str
    calc: str


Layer = Union[SourceLayer, DerivedLayer]


class DataEnvironment(BaseModel):
    layers: List[Layer]
