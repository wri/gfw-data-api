from pydantic import BaseModel
from typing import List, Optional

from .base import Base
from .metadata import Metadata
from .version import Version


class Dataset(Base):
    dataset: str
    metadata: Metadata
    versions: Optional[List[Version]]


class DatasetCreateIn(BaseModel):
    metadata: Metadata


class DatasetUpdateIn(BaseModel):
    # dataset: Optional[str]
    metadata: Metadata
