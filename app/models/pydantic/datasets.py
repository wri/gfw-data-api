from typing import List, Optional

from .base import BaseRecord, StrictBaseModel
from .metadata import DatasetMetadata
from .responses import Response


class Dataset(BaseRecord):
    dataset: str
    metadata: DatasetMetadata
    versions: Optional[List[str]] = list()


class DatasetCreateIn(StrictBaseModel):
    metadata: DatasetMetadata


class DatasetUpdateIn(StrictBaseModel):
    metadata: DatasetMetadata


class DatasetResponse(Response):
    data: Dataset


class DatasetsResponse(Response):
    data: List[Dataset]
