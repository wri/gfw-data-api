from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel

from .base import Base
from .responses import Response


class Geometry(BaseModel):
    type: str
    coordinates: List[Any]


class Feature(BaseModel):
    properties: Dict[str, Any]
    type: str
    geometry: Geometry


class FeatureCollection(BaseModel):
    features: List[Feature]
    crs: Optional[Dict[str, Any]]
    type: str


class Geostore(Base):
    gfw_geostore_id: UUID
    gfw_geojson: FeatureCollection
    gfw_area__ha: float
    gfw_bbox: List[float]


class GeostoreIn(FeatureCollection):
    pass


class GeostoreOut(Base):
    gfw_geostore_id: UUID
    gfw_geojson: str
    gfw_area__ha: float
    gfw_bbox: List[float]


class GeostoreResponse(Response):
    data: GeostoreOut
