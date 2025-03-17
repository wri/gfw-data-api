import csv
from abc import ABC, abstractmethod
from enum import Enum
from io import StringIO
from typing import Dict, Optional, Union
from uuid import UUID

import pandas as pd
from pydantic import Field

from app.models.pydantic.responses import Response

from .base import StrictBaseModel


class AreaOfInterest(StrictBaseModel, ABC):
    @abstractmethod
    def get_geostore_id(self) -> UUID:
        """Return the unique identifier for the area of interest."""
        pass


class GeostoreAreaOfInterest(AreaOfInterest):
    geostore_id: UUID = Field(..., title="Geostore ID")

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
