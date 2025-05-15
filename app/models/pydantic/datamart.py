import csv
from enum import Enum
from io import StringIO
from itertools import groupby
from typing import Any, Dict, List, Literal, Optional, Union
from uuid import UUID

from fastapi import HTTPException, Request
from jsonschema import ValidationError
from pydantic import Field, root_validator, validator

from app.models.pydantic.responses import Response

from ...crud.geostore import get_gadm_geostore_id, get_wdpa_geostore_id
from ...crud.versions import get_latest_version
from ...errors import RecordNotFoundError
from .base import StrictBaseModel


class AreaOfInterest(StrictBaseModel):
    async def get_geostore_id(self) -> UUID:
        """Return the unique identifier for the area of interest."""
        raise NotImplementedError("This method is not implemented.")


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
    simplify: Optional[float] = Field(None, title="Simplification factor for geometry")

    async def get_geostore_id(self) -> UUID:
        admin_level = self.get_admin_level()
        geostore_id = await get_gadm_geostore_id(
            admin_provider=self.provider,
            admin_version=self.version,
            adm_level=admin_level,
            country_id=self.country,
            region_id=self.region,
            subregion_id=self.subregion,
        )
        return geostore_id

    def get_admin_level(self):
        admin_level = (
            sum(
                1
                for field in (self.country, self.region, self.subregion)
                if field is not None
            )
            - 1
        )
        return admin_level

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


class WdpaAreaOfInterest(AreaOfInterest):
    type: Literal["protected_area"] = "protected_area"
    wdpa_id: str = Field(..., title="World Database on Protected Areas (WDPA) ID")

    async def get_geostore_id(self) -> UUID:
        dataset = "wdpa_protected_areas"
        try:
            latest_version = await get_latest_version(dataset)
        except RecordNotFoundError:
            raise HTTPException(
                status_code=404, detail="WDPA dataset does not have latest version."
            )
        return await get_wdpa_geostore_id(dataset, latest_version, self.wdpa_id)


class Global(AreaOfInterest):
    type: Literal["global"] = Field(
        "global",
        description="Apply analysis to the full spatial extent of the dataset.",
    )


class AnalysisStatus(str, Enum):
    saved = "saved"
    pending = "pending"
    failed = "failed"


class DataMartSource(StrictBaseModel):
    dataset: str
    version: str


class DataMartMetadata(StrictBaseModel):
    aoi: Union[GeostoreAreaOfInterest, AdminAreaOfInterest, Global, WdpaAreaOfInterest]
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
    aoi: Union[
        GeostoreAreaOfInterest, AdminAreaOfInterest, Global, WdpaAreaOfInterest
    ] = Field(..., discriminator="type")
    canopy_cover: int = 30
    dataset_version: Dict[str, str] = {}


class TreeCoverLossByDriverMetadata(DataMartMetadata):
    canopy_cover: int


class TreeCoverLossByDriverResult(StrictBaseModel):
    tree_cover_loss_by_driver: List[Dict[str, Any]]
    yearly_tree_cover_loss_by_driver: List[Dict[str, Any]]

    @staticmethod
    def from_rows(
        rows,
        drivers_key: str = "tsc_tree_cover_loss_drivers__driver",
        driver_value_map: Dict[str, int] | None = None,
    ):
        yearly_tcl_by_driver = [
            {
                "drivers_type": row[drivers_key],
                "loss_year": row["umd_tree_cover_loss__year"],
                "loss_area_ha": row["area__ha"],
                "gross_carbon_emissions_Mg": row[
                    "gfw_forest_carbon_gross_emissions__Mg_CO2e"
                ],
            }
            for row in rows
        ]

        # sort rows first since groupby will only group consecutive keys
        # this shouldn't matter, but to match existing sorting behavior, sort by
        # mapped pixel value rather than alphabetical
        if driver_value_map is None:
            driver_value_map = {
                "Unknown": 0,
                "Permanent agriculture": 1,
                "Commodity driven deforestation": 2,
                "Shifting agriculture": 3,
                "Forestry": 4,
                "Wildfire": 5,
                "Urbanization": 6,
                "Other natural disturbances": 7,
            }
        sorted_rows = sorted(
            rows,
            key=lambda x: driver_value_map[x[drivers_key]],
        )
        tcl_by_driver = [
            {
                "drivers_type": driver,
                "loss_area_ha": sum([year["area__ha"] for year in years]),
                "gross_carbon_emissions_Mg": sum(
                    [
                        year["gfw_forest_carbon_gross_emissions__Mg_CO2e"]
                        for year in years
                    ]
                ),
            }
            for driver, groups in groupby(sorted_rows, key=lambda x: x[drivers_key])
            for years in [list(groups)]
        ]

        return TreeCoverLossByDriverResult(
            tree_cover_loss_by_driver=tcl_by_driver,
            yearly_tree_cover_loss_by_driver=yearly_tcl_by_driver,
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
            ["drivers_type", "loss_year", "loss_area_ha", "gross_carbon_emissions_Mg"]
        )

        if self.data.status == "saved":
            for row in self.data.result.yearly_tree_cover_loss_by_driver:
                wr.writerow(
                    [
                        row["drivers_type"],
                        row["loss_year"],
                        row["loss_area_ha"],
                        row["gross_carbon_emissions_Mg"],
                    ]
                )

        csv_file.seek(0)
        return csv_file


def parse_area_of_interest(request: Request) -> AreaOfInterest:
    params = request.query_params
    aoi_type = params.get("aoi[type]")
    try:
        if aoi_type == "geostore":
            return GeostoreAreaOfInterest(
                geostore_id=params.get("aoi[geostore_id]", None)
            )

            # Otherwise, check if the request contains admin area information
        if aoi_type == "admin":
            return AdminAreaOfInterest(
                country=params.get("aoi[country]", None),
                region=params.get("aoi[region]", None),
                subregion=params.get("aoi[subregion]", None),
                provider=params.get("aoi[provider]", None),
                version=params.get("aoi[version]", None),
                simplify=params.get("aoi[simplify]", None),
            )

        if aoi_type == "global":
            return Global()

        if aoi_type == "protected_area":
            return WdpaAreaOfInterest(wdpa_id=params.get("aoi[wdpa_id]"))

        # If neither type is provided, raise an error
        raise HTTPException(
            status_code=422, detail="Invalid Area of Interest parameters"
        )
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())
