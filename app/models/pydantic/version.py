from enum import Enum
from typing import Optional, List, Tuple, Dict, Any

from pydantic import BaseModel

from .base import Base
from .metadata import VersionMetadata
from .source import SourceType


class Version(Base):
    dataset: str
    version: str
    is_latest: bool = False
    is_mutable: bool = False
    source_type: SourceType
    source_uri: Optional[List[str]] = None
    copy_source: bool = False
    has_vector_tile_cache: bool = False
    has_raster_tile_cache: bool = False
    has_geostore: bool = False
    has_feature_info: bool = False
    has_sql_query: bool = False
    metadata: VersionMetadata
    assets: List[Tuple[str, str]] = list()
    history: List[Dict[str, Any]]


class VersionCreateIn(BaseModel):
    is_latest: bool = False
    source_type: SourceType
    source_uri: Optional[List[str]]
    metadata: VersionMetadata


class VersionUpdateIn(BaseModel):
    is_latest: Optional[bool]
    source_type: Optional[SourceType]
    source_uri: Optional[List[str]]
    metadata: Optional[VersionMetadata]



