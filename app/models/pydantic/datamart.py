from typing import Dict, Optional
from uuid import UUID

from app.models.pydantic.responses import Response

from .base import StrictBaseModel


class DataMartSource(StrictBaseModel):
    dataset: str
    version: str


class DataMartMetadata(StrictBaseModel):
    geostore_id: UUID
    sources: list[DataMartSource]


class DataMartResource(StrictBaseModel):
    status: str = "saved"
    details: Optional[str] = None
    metadata: DataMartMetadata = None


class DataMartResourceLink(StrictBaseModel):
    link: str


class DataMartResourceLinkResponse(Response):
    data: DataMartResourceLink


class TreeCoverLossByDriverIn(StrictBaseModel):
    geostore_id: UUID
    canopy_cover: int = 30
    dataset_version: Dict[str, str] = {}


class TreeCoverLossByDriverMetadata(DataMartMetadata):
    canopy_cover: int


class TreeCoverLossByDriver(DataMartResource):
    treeCoverLossByDriver: Optional[Dict[str, float]] = None
    metadata: Optional[TreeCoverLossByDriverMetadata] = None


class TreeCoverLossByDriverResponse(Response):
    data: TreeCoverLossByDriver
