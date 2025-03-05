from enum import Enum
from typing import Dict, Optional
from uuid import UUID
from pydantic import Field

from app.models.pydantic.responses import Response
from .base import StrictBaseModel


class AnalysisStatus(str, Enum):
    saved = "saved"
    pending = "pending"
    failed = "failed"


class DataMartSource(StrictBaseModel):
    dataset: str
    version: str


class DataMartMetadata(StrictBaseModel):
    geostore_id: UUID
    sources: list[DataMartSource]


class DataMartResource(StrictBaseModel):
    id: UUID
    status: AnalysisStatus
    error: Optional[str] = None
    requested_by: Optional[UUID] = None
    endpoint: str
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


class TreeCoverLossByDriver(StrictBaseModel):
    result: Optional[Dict[str, float]] = Field(None, alias="tree_cover_loss_by_driver")
    metadata: Optional[TreeCoverLossByDriverMetadata] = None
    error: Optional[str] = None

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class TreeCoverLossByDriverUpdate(StrictBaseModel):
    result: Optional[Dict[str, float]] = Field(None, alias="tree_cover_loss_by_driver")
    metadata: Optional[TreeCoverLossByDriverMetadata] = None
    status: Optional[AnalysisStatus] = AnalysisStatus.saved
    error: Optional[str] = None

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class TreeCoverLossByDriverResponse(Response):
    data: TreeCoverLossByDriver
