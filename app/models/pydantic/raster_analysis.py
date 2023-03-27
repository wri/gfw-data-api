from typing import List, Optional, Union

from ..enum.pixetl import Grid
from .asset_metadata import RasterTable
from .base import StrictBaseModel
from .creation_options import NoDataType


class BaseLayer(StrictBaseModel):
    name: str
    no_data: Optional[NoDataType]


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
