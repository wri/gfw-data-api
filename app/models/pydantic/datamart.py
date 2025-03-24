import csv
from abc import ABC, abstractmethod
from enum import Enum
from io import StringIO
from typing import Dict, Literal, Optional, Union
from uuid import UUID

import pandas as pd
from pydantic import Field, root_validator, validator

from app.models.pydantic.responses import Response

from ...crud.geostore import get_gadm_geostore_id
from .base import StrictBaseModel


class AreaOfInterest(StrictBaseModel, ABC):
    @abstractmethod
    async def get_geostore_id(self) -> UUID:
        """Return the unique identifier for the area of interest."""
        pass


class GeostoreAreaOfInterest(AreaOfInterest):
    type: Literal["geostore"] = "geostore"
    geostore_id: UUID = Field(..., title="Geostore ID")

    async def get_geostore_id(self) -> UUID:
        return self.geostore_id


class AdminAreaOfInterest(AreaOfInterest):
    type: Literal["admin"] = "admin"
    country: str = Field(..., title="ISO Country Code")
    region: Optional[str] = Field(None, title="Region")
    subregion: Optional[str] = Field(None, title="Subregion")
    provider: str = Field("gadm", title="Administrative Boundary Provider")
    version: str = Field("4.1", title="Administrative Boundary Version")

    async def get_geostore_id(self) -> UUID:
        admin_level = (
            sum(
                1
                for field in (self.country, self.region, self.subregion)
                if field is not None
            )
            - 1
        )
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

    @validator("provider", pre=True, always=True)
    def set_provider_default(cls, v):
        return v or "gadm"

    @validator("version", pre=True, always=True)
    def set_version_default(cls, v):
        return v or "4.1"


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
    metadata: Optional[DataMartMetadata] = None


class DataMartResourceLink(StrictBaseModel):
    link: str


class DataMartResourceLinkResponse(Response):
    data: DataMartResourceLink


class TreeCoverLossByDriverIn(StrictBaseModel):
    aoi: Union[GeostoreAreaOfInterest, AdminAreaOfInterest] = Field(
        ..., discriminator="type"
    )
    canopy_cover: int = 30
    dataset_version: Dict[str, str] = {}


class TreeCoverLossByDriverMetadata(DataMartMetadata):
    canopy_cover: int


class TreeCoverLossByDriverResult(StrictBaseModel):
    tree_cover_loss_by_driver: Dict[str, float]
    yearly_tree_cover_loss_by_driver: Dict[str, Dict[str, float]]

    @staticmethod
    def from_rows(rows):
        df = pd.DataFrame(rows)
        by_year = (
            df.groupby(["umd_tree_cover_loss__year"])
            .apply(
                lambda x: dict(
                    zip(x["tsc_tree_cover_loss_drivers__driver"], x["area__ha"])
                )
            )
            .to_dict()
        )
        by_driver = (
            df.drop(["umd_tree_cover_loss__year"], axis=1)
            .groupby(["tsc_tree_cover_loss_drivers__driver"])
            .sum()
            .to_dict()["area__ha"]
        )
        return TreeCoverLossByDriverResult(
            tree_cover_loss_by_driver=by_driver,
            yearly_tree_cover_loss_by_driver=by_year,
        )


class TreeCoverLossByDriver(StrictBaseModel):
    result: Optional[TreeCoverLossByDriverResult] = None
    metadata: Optional[TreeCoverLossByDriverMetadata] = None
    message: Optional[str] = None
    status: AnalysisStatus

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class TreeCoverLossByDriverUpdate(StrictBaseModel):
    result: Optional[TreeCoverLossByDriverResult] = None
    metadata: Optional[TreeCoverLossByDriverMetadata] = None
    status: Optional[AnalysisStatus] = AnalysisStatus.saved
    message: Optional[str] = None

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class TreeCoverLossByDriverResponse(Response):
    data: TreeCoverLossByDriver

    def to_csv(
        self,
    ) -> StringIO:
        """Create a new csv file that represents the resource Response will
        return a temporary redirect to download URL."""
        csv_file = StringIO()
        wr = csv.writer(csv_file, quoting=csv.QUOTE_NONNUMERIC)
        wr.writerow(
            [
                "umd_tree_cover_loss__year",
                "tsc_tree_cover_loss_drivers__driver",
                "area__ha",
            ]
        )

        if self.data.status == "saved":
            for (
                year,
                tcl_by_driver,
            ) in self.data.result.yearly_tree_cover_loss_by_driver.items():
                for driver, area in tcl_by_driver.items():
                    wr.writerow([year, driver, area])

        csv_file.seek(0)
        return csv_file
