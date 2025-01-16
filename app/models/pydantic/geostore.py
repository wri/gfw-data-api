import json
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import validator

from ..enum.geostore import LandUseTypeUseString
from .base import BaseRecord, StrictBaseModel
from .responses import Response


class Geometry(StrictBaseModel):
    type: str
    coordinates: List[Any]


class Feature(StrictBaseModel):
    properties: Optional[Dict[str, Any]]
    type: str
    geometry: Optional[Geometry]


class FeatureCollection(StrictBaseModel):
    crs: Optional[Dict[str, Any]]
    type: str
    features: List[Feature]


class Geostore(BaseRecord):
    gfw_geostore_id: UUID
    gfw_geojson: Geometry
    gfw_area__ha: float
    gfw_bbox: List[float]

    @validator("gfw_geojson", pre=True)
    def convert_to_dict(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        else:
            return v


class GeostoreCommon(StrictBaseModel):
    geostore_id: UUID
    geojson: Geometry
    area__ha: float
    bbox: List[float]


class GeostoreIn(StrictBaseModel):
    geometry: Geometry


class GeostoreResponse(Response):
    data: Geostore


class RWCalcAreaForGeostoreIn(StrictBaseModel):
    properties: Dict
    type: str
    geometry: Geometry


class RWCalcAreaForGeostoreAttributes(StrictBaseModel):
    bbox: List[float]
    areaHA: float


class RWCalcAreaForGeostoreData(StrictBaseModel):
    type: Literal["geomArea"]
    attributes: RWCalcAreaForGeostoreAttributes


class RWCalcAreaForGeostoreResponse(StrictBaseModel):
    data: RWCalcAreaForGeostoreData


class RWFindByIDsIn(StrictBaseModel):
    geostores: List[str]


class RWViewGeostoreResponse(StrictBaseModel):
    view_link: str


class AdminBoundaryInfo(StrictBaseModel):
    use: Dict
    simplifyThresh: float
    gadm: str  # TODO: Make an enum?
    name: str
    id2: int
    id1: int
    iso: str


class LandUseUse(StrictBaseModel):
    use: LandUseTypeUseString
    id: int


class LandUseInfo(StrictBaseModel):
    use: LandUseUse
    simplify: bool


class WDPAInfo(StrictBaseModel):
    use: Dict
    wdpaid: int


class RWAdminListItem(StrictBaseModel):
    geostoreId: str
    iso: str
    name: str


class RWAdminListResponse(StrictBaseModel):
    data: List[RWAdminListItem]


class RWGeostoreAttributes(StrictBaseModel):
    geojson: FeatureCollection
    hash: str
    provider: Dict
    areaHa: float
    bbox: List[float]
    lock: bool
    info: AdminBoundaryInfo | LandUseInfo | WDPAInfo


class RWGeostore(StrictBaseModel):
    type: Literal["geoStore"]
    id: str
    attributes: RWGeostoreAttributes


class RWGeostoreResponse(StrictBaseModel):
    data: RWGeostore
