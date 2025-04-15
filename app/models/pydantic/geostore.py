import json
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import validator

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
    features: List[Feature]
    crs: Optional[Dict[str, Any]]
    type: str


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


class RWGeostoreIn(StrictBaseModel):
    geojson: Geometry | Feature | FeatureCollection


class GeostoreResponse(Response):
    data: Geostore


class Adm0BoundaryInfo(StrictBaseModel):
    use: Dict
    simplifyThresh: Optional[float]
    gadm: str
    name: str
    iso: str


class Adm1BoundaryInfo(Adm0BoundaryInfo):
    id1: int


class Adm2BoundaryInfo(Adm1BoundaryInfo):
    id2: int


class CreateGeostoreResponseInfo(StrictBaseModel):
    use: Dict


class AdminListItem(StrictBaseModel):
    geostoreId: str
    iso: str


class AdminListItemWithName(AdminListItem):
    name: str


class AdminListResponse(Response):
    data: List[AdminListItem | AdminListItemWithName]


class WDPAInfo(StrictBaseModel):
    use: Dict
    wdpaid: int


class LandUseUse(StrictBaseModel):
    use: str
    id: int


class LandUseInfo(StrictBaseModel):
    use: LandUseUse
    simplify: bool


class AdminGeostoreAttributes(StrictBaseModel):
    geojson: FeatureCollection
    hash: str
    provider: Dict
    areaHa: float
    bbox: List[float]
    lock: bool
    info: (
        Adm2BoundaryInfo
        | Adm1BoundaryInfo
        | Adm0BoundaryInfo
        | CreateGeostoreResponseInfo
        | LandUseInfo
        | WDPAInfo
    )


class AdminGeostore(StrictBaseModel):
    type: Literal["geoStore"]
    id: str
    attributes: AdminGeostoreAttributes


class AdminGeostoreResponse(Response):
    data: AdminGeostore
