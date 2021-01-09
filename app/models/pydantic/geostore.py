from typing import Any, Dict, List, Optional
from uuid import UUID

from .base import BaseRecord, StrictBaseModel
from .responses import Response


class Geometry(StrictBaseModel):
    type: str
    coordinates: List[Any]


class Feature(StrictBaseModel):
    properties: Dict[str, Any]
    type: str
    geometry: Optional[Geometry]


class FeatureCollection(StrictBaseModel):
    features: List[Feature]
    crs: Optional[Dict[str, Any]]
    type: str


class Geostore(BaseRecord):
    gfw_geostore_id: UUID
    gfw_geojson: str
    gfw_area__ha: float
    gfw_bbox: List[float]


class GeostoreHydrated(BaseRecord):
    gfw_geostore_id: UUID
    gfw_geojson: FeatureCollection
    gfw_area__ha: float
    gfw_bbox: List[float]


class GeostoreIn(FeatureCollection):
    pass


class GeostoreResponse(Response):
    data: GeostoreHydrated
