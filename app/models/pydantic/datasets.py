from typing import List, Optional, Union

from pydantic import Field, BaseModel

from .base import BaseRecord, StrictBaseModel
from .metadata import DatasetMetadata, DatasetMetadataOut, DatasetMetadataUpdate
from .responses import Response


class Dataset(BaseRecord):
    dataset: str
    is_downloadable: bool
    metadata: Union[DatasetMetadataOut, BaseModel]
    versions: Optional[List[str]] = list()


class DatasetCreateIn(StrictBaseModel):
    is_downloadable: bool = Field(
        True,
        description="Flag to specify if assets associated with dataset can be downloaded."
        "All associated versions and assets will inherit this value. "
        "Value can be overridden at version  or asset level.",
    )
    metadata: DatasetMetadata


class DatasetUpdateIn(StrictBaseModel):
    is_downloadable: Optional[bool]
    metadata: Optional[DatasetMetadataUpdate]


class DatasetResponse(Response):
    data: Dataset


class DatasetsResponse(Response):
    data: List[Dataset]
