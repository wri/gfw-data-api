from typing import Any, Dict, List, Optional
from uuid import UUID

from .base import Base, DataApiBaseModel
from .responses import Response


class Geometry(DataApiBaseModel):
    type: str
    coordinates: List[Any]


class Feature(DataApiBaseModel):
    properties: Dict[str, Any]
    type: str
    geometry: Optional[Geometry]


class FeatureCollection(DataApiBaseModel):
    features: List[Feature]
    crs: Optional[Dict[str, Any]]
    type: str


class Geostore(Base):
    gfw_geostore_id: UUID
    gfw_geojson: str
    gfw_area__ha: float
    gfw_bbox: List[float]


class GeostoreHydrated(Base):
    gfw_geostore_id: UUID
    gfw_geojson: FeatureCollection
    gfw_area__ha: float
    gfw_bbox: List[float]


class GeostoreIn(FeatureCollection):
    pass


class GeostoreResponse(Response):
    data: GeostoreHydrated
