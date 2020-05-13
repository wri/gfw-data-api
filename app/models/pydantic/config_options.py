from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class IndexType(str, Enum):
    gist = "gist"
    btree = "btree"
    hash = "hash"


class Index(BaseModel):
    index_type: IndexType
    column_name: str


class VectorSourceConfigOptions(BaseModel):
    src_driver: str
    zipped: bool
    layers: Optional[List[str]] = None
    indices: List[Index] = [
        Index(index_type="gist", column_name="geom"),
        Index(index_type="gist", column_name="geom_wm"),
        Index(index_type="hash", column_name="gfw_geostore_id"),
    ]
