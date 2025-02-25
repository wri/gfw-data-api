from typing import Dict
from uuid import UUID

from .base import StrictBaseModel


class TreeCoverLossByDriverIn(StrictBaseModel):
    geostore_id: UUID
    canopy_cover: int = 30


class DataMartSource(StrictBaseModel):
    dataset: str
    version: str


class DataMartMetadata(StrictBaseModel):
    geostore_id: UUID
    sources: list[DataMartSource]


class DataMartResource(StrictBaseModel):
    status: str = "saved"
    metadata: DataMartMetadata


class TreeCoverLossByDriverMetadata(DataMartMetadata):
    canopy_cover: int


class TreeCoverLossByDriver(DataMartResource):
    treeCoverLossByDriver: Dict[str, float]
    metadata: TreeCoverLossByDriverMetadata
