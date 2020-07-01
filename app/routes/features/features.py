"""Explore data entries for a given dataset version (vector and tabular data
only) in a classic RESTful way."""
from collections import defaultdict
from functools import partial
from typing import DefaultDict, Optional

import pyproj
from fastapi import APIRouter, Depends, Path, Query
from fastapi.responses import ORJSONResponse
from shapely.geometry import Point, Polygon
from shapely.ops import transform
from sqlalchemy import column, select, table, text
from sqlalchemy.sql.elements import TextClause

from ...application import ContextEngine, db
from ...models.orm.queries.fields import fields
from ...routes import dataset_dependency, version_dependency

router = APIRouter()


@router.get("/features/{dataset}/{version}", response_class=ORJSONResponse)
async def get_features(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    lat: float = Query(None, title="Latitude", ge=-90, le=90),
    lng: float = Query(None, title="Longitude", ge=-180, le=180),
    z: int = Query(None, title="Zoom level", ge=0, le=22),
):
    """Retrieve list of features Add optional spatial filter using a point
    buffer (for info tool)."""
    return await get_features_by_location(dataset, version, lat, lng, z)


@router.get(
    "/feature/{feature_id}", response_class=ORJSONResponse, tags=["Features"],
)
async def get_feature(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    feature_id: int = Path(..., title="Feature ID", ge=0),
):
    """Retrieve attribute values for a given feature."""

    pass


async def get_features_by_location(dataset, version, lat, lng, zoom):
    t = table(version)  # TODO validate version
    t.schema = dataset

    buffer_distance = _get_buffer_distance(zoom)
    if buffer_distance:
        geometry = Polygon(geodesic_point_buffer(lat, lng, buffer_distance))
    else:
        geometry = Point((lat, lng))

    _fields = await get_fields(dataset, version)

    columns = [column(field["name"]) for field in _fields if field["is_feature_info"]]

    features = (
        db.select(columns)
        .select_from(t)
        .where(filter_intersects("geom", str(geometry)))
    )

    return features


def geodesic_point_buffer(lat, lng, meter):
    """https://gis.stackexchange.com/questions/289044/creating-buffer-circle-x-
    kilometers-from-point-using-python."""
    proj_wgs84 = pyproj.Proj(init="epsg:4326")
    # Azimuthal equidistant projection
    aeqd_proj = "+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0"
    project = partial(
        pyproj.transform, pyproj.Proj(aeqd_proj.format(lat=lat, lon=lng)), proj_wgs84
    )
    buf = Point(0, 0).buffer(meter)  # distance in metres

    return [transform(project, buf).exterior.coords[:]]


def _get_buffer_distance(zoom: int) -> Optional[int]:
    zoom_buffer: DefaultDict[str, Optional[int]] = defaultdict(lambda: None)
    zoom_buffer.update(
        {
            "z0": 10000,
            "z1": 5000,
            "z2": 2500,
            "z3": 1250,
            "z4": 600,
            "z5": 300,
            "z6": 500,
            "z7": 80,
            "z8": 40,
            "z9": 20,
        }
    )
    return zoom_buffer[zoom]


def filter_intersects(field, geometry) -> TextClause:
    f = text(f"ST_Intersects({field}, ST_SetSRID(ST_GeomFromGeoJSON(:geometry),4326))")
    value = {"geometry": f"{geometry}"}
    f = f.bindparams(**value)

    return f


async def get_fields(dataset, version):
    async with ContextEngine("READ"):
        rows = await db.all(fields, dataset=dataset, version=version)
    return rows
