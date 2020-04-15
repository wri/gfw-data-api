from pydantic import BaseModel
from typing import Optional

from .base import Base
from .metadata import Metadata


class Dataset(Base):
    dataset: str
    metadata: Metadata


class DatasetCreateIn(BaseModel):
    metadata: Metadata


class DatasetUpdateIn(BaseModel):
    dataset: Optional[str]
    metadata: Optional[Metadata]
