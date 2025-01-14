import json
from typing import Any, Dict, List, Optional, Literal
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


class GeostoreResponse(Response):
    data: Geostore


class RWCalcAreaForGeostoreIn(StrictBaseModel):
    type: str
    geometry: Geometry
    properties: Dict


class RWFindByIDsIn(StrictBaseModel):
    geostores: List[str]


class RWViewGeostore(StrictBaseModel):
    view_link: str


class WDPAInfo(StrictBaseModel):
    use: Dict
    wdpaid: int


class RWGeostoreAttributes(StrictBaseModel):
    geojson: FeatureCollection
    hash: str
    provider: Dict
    areaHa: float
    bbox: List[float]
    lock: bool
    info: WDPAInfo


class RWGeostore(StrictBaseModel):
    type: Literal["geoStore"]
    id: str
    attributes: RWGeostoreAttributes


class RWGeostoreResponse(StrictBaseModel):
    data: RWGeostore
