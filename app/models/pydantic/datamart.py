from enum import Enum
from typing import Dict, Optional, Union
from uuid import UUID
from abc import ABC, abstractmethod

from pydantic import Field

from app.models.pydantic.responses import Response

from .base import StrictBaseModel

class AreaOfInterest(StrictBaseModel, ABC):
    @abstractmethod
    def get_geostore_id(self) -> UUID:
        """Return the unique identifier for the area of interest."""
        pass


class GeostoreAreaOfInterest(AreaOfInterest):
    geostore_id:UUID = Field(..., title="Geostore ID")

    def get_geostore_id(self) -> UUID:
        return self.geostore_id


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
    message: Optional[str] = None
    requested_by: Optional[UUID] = None
    endpoint: str
    metadata: DataMartMetadata = None


class DataMartResourceLink(StrictBaseModel):
    link: str


class DataMartResourceLinkResponse(Response):
    data: DataMartResourceLink


class TreeCoverLossByDriverIn(StrictBaseModel):
    aoi: Union[GeostoreAreaOfInterest]
    canopy_cover: int = 30
    dataset_version: Dict[str, str] = {}


class TreeCoverLossByDriverMetadata(DataMartMetadata):
    canopy_cover: int


class TreeCoverLossByDriver(StrictBaseModel):
    result: Optional[Dict[str, float]] = Field(None, alias="tree_cover_loss_by_driver")
    metadata: Optional[TreeCoverLossByDriverMetadata] = None
    message: Optional[str] = None
    status: AnalysisStatus

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class TreeCoverLossByDriverUpdate(StrictBaseModel):
    result: Optional[Dict[str, float]] = Field(None, alias="tree_cover_loss_by_driver")
    metadata: Optional[TreeCoverLossByDriverMetadata] = None
    status: Optional[AnalysisStatus] = AnalysisStatus.saved
    message: Optional[str] = None

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class TreeCoverLossByDriverResponse(Response):
    data: TreeCoverLossByDriver



