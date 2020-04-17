from pydantic import BaseModel
from typing import Optional, List

from .base import Base
from .metadata import Metadata
from ..orm.version import Version


class Dataset(Base):
    dataset: str
    metadata: Metadata


class DatasetCreateIn(BaseModel):
    metadata: Metadata


class DatasetUpdateIn(BaseModel):
    # dataset: Optional[str]
    metadata: Metadata
