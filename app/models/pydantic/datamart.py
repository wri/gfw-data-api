from enum import Enum
from typing import Dict, Literal, Optional, Union
from uuid import UUID

from pydantic import Field, root_validator, validator

from app.models.pydantic.responses import Response

from .base import StrictBaseModel
from ...crud.geostore import get_gadm_geostore_id


class AreaOfInterest(StrictBaseModel):
    async def get_geostore_id(self) -> UUID:
        """Return the unique identifier for the area of interest."""
        raise NotImplementedError("This method is not implemented.")


class GeostoreAreaOfInterest(AreaOfInterest):
    type: Literal['geostore'] = 'geostore'
    geostore_id: UUID = Field(..., title="Geostore ID")

    async def get_geostore_id(self) -> UUID:
        return self.geostore_id


class AdminAreaOfInterest(AreaOfInterest):
    type: Literal['admin'] = 'admin'
    country: str = Field(..., title="ISO Country Code")
    region: Optional[str] = Field(None, title="Region")
    subregion: Optional[str] = Field(None, title="Subregion")
    provider: str = Field('gadm', title="Administrative Boundary Provider")
    version: str = Field('4.1', title="Administrative Boundary Version")

    async def get_geostore_id(self) -> UUID:
        admin_level = sum(1 for field in (self.country, self.region, self.subregion) if field is not None) - 1
        geostore_id = await get_gadm_geostore_id(
            admin_provider=self.provider,
            admin_version=self.version,
            adm_level=admin_level,
            country_id=self.country,
            region_id=self.region,
            subregion_id=self.subregion,
        )
        return UUID(geostore_id)

    @root_validator
    def check_region_subregion(cls, values):
        region = values.get("region")
        subregion = values.get("subregion")
        if subregion is not None and region is None:
            raise ValueError("region must be specified if subregion is provided")
        return values

    @validator('provider', pre=True, always=True)
    def set_provider_default(cls, v):
        return v or 'gadm'

    @validator('version', pre=True, always=True)
    def set_version_default(cls, v):
        return v or '4.1'


class Global(AreaOfInterest):
    type: Literal["global"] = "global"


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
    aoi: Union[GeostoreAreaOfInterest, AdminAreaOfInterest, Global] = Field(..., discriminator='type')
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
