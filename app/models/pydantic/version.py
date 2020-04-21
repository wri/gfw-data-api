from enum import Enum
from typing import Optional, List, Tuple

from pydantic import BaseModel

from .asset import Asset
from .base import Base
from .metadata import Metadata


class SourceType(str, Enum):
    vector = "vector"
    raster = "raster"


class Version(Base):
    dataset: str
    version: str
    is_latest: bool = False
    source_type: SourceType
    has_vector_tile_cache: bool = False
    has_raster_tile_cache: bool = False
    has_geostore: bool = False
    has_feature_info: bool = False
    has_10_40000_tiles: bool = False
    has_90_27008_tiles: bool = False
    has_90_9876_tiles: bool = False
    metadata: Metadata
    assets: List[Tuple[str]] = list()


class VersionCreateIn(BaseModel):
    is_latest: bool = False
    source_type: SourceType
    has_vector_tile_cache: bool = False
    has_raster_tile_cache: bool = False
    has_geostore: bool = False
    has_feature_info: bool = False
    has_10_40000_tiles: bool = False
    has_90_27008_tiles: bool = False
    has_90_9876_tiles: bool = False
    metadata: Metadata


class VersionUpdateIn(BaseModel):
    is_latest: Optional[bool]
    source_type: Optional[SourceType]
    has_vector_tile_cache: Optional[bool]
    has_raster_tile_cache: Optional[bool]
    has_geostore: Optional[bool]
    has_feature_info: Optional[bool]
    has_10_40000_tiles: Optional[bool]
    has_90_27008_tiles: Optional[bool]
    has_90_9876_tiles: Optional[bool]
    metadata: Optional[Metadata]



