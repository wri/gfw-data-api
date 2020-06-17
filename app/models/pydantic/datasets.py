from typing import List, Optional

from pydantic import BaseModel

from .base import Base
from .metadata import DatasetMetadata
from .responses import Response


class Dataset(Base):
    dataset: str
    metadata: DatasetMetadata
    versions: Optional[List[str]] = list()


class DatasetCreateIn(BaseModel):
    metadata: DatasetMetadata


class DatasetUpdateIn(BaseModel):
    metadata: DatasetMetadata


class DatasetResponse(Response):
    data: Dataset


class DatasetsResponse(Response):
    data: List[Dataset]
