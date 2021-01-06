from typing import List, Optional

from .base import Base, DataApiBaseModel
from .metadata import DatasetMetadata
from .responses import Response


class Dataset(Base):
    dataset: str
    metadata: DatasetMetadata
    versions: Optional[List[str]] = list()


class DatasetCreateIn(DataApiBaseModel):
    metadata: DatasetMetadata


class DatasetUpdateIn(DataApiBaseModel):
    metadata: DatasetMetadata


class DatasetResponse(Response):
    data: Dataset


class DatasetsResponse(Response):
    data: List[Dataset]
