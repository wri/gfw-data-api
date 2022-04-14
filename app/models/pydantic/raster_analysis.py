from typing import List, Optional, Union

from ..enum.pixetl import Grid
from .base import StrictBaseModel
from .asset_metadata import RasterTable


class BaseLayer(StrictBaseModel):
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


class DataEnvironment(StrictBaseModel):
    layers: List[Layer]
