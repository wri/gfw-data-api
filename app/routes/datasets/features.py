"""Explore data entries for a given dataset version (vector and tabular data
only) in a classic RESTful way."""
from collections import defaultdict
from functools import partial
from typing import DefaultDict, Optional

import pyproj
from asyncpg import UndefinedTableError
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import ORJSONResponse
from geojson import Point as geoPoint
from geojson import Polygon as geoPolygon
from shapely.geometry import Point
from shapely.ops import transform
from sqlalchemy.sql.elements import TextClause

from ...application import db
from ...crud import assets
from ...models.pydantic.features import FeaturesResponse
from ...routes import dataset_dependency, version_dependency

router = APIRouter()


@router.get(
    "/{dataset}/{version}/features", response_class=ORJSONResponse, tags=["Versions"]
)
async def get_features(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    lat: float = Query(..., title="Latitude", ge=-90, le=90),
    lng: float = Query(..., title="Longitude", ge=-180, le=180),
    z: int = Query(..., title="Zoom level", ge=0, le=22),
):
    """Retrieve list of features for a given spatial location.

    Search radius various decreases for higher zoom levels.
    """

    try:
        feature_rows = await get_features_by_location(dataset, version, lat, lng, z)
    except UndefinedTableError:
        raise HTTPException(
            status_code=501,
            detail=f"Endpoint not implement for {dataset}.{version}."
            "Not a table or vector asset.",
        )

    return await _features_response(feature_rows)


async def get_features_by_location(dataset, version, lat, lng, zoom):
    t = db.table(version)
    t.schema = dataset

    geometry = geodesic_point_buffer(lat, lng, zoom)

    all_columns = await get_fields(dataset, version)
    feature_columns = [
        db.column(field["field_name"])
        for field in all_columns
        if field["is_feature_info"]
    ]

    sql = (
        db.select(feature_columns)
        .select_from(t)
        .where(filter_intersects("geom", str(geometry)))
    )

    features = await db.all(sql)

    return features


def filter_intersects(field, geometry) -> TextClause:
    f = db.text(
        f"ST_Intersects({field}, ST_SetSRID(ST_GeomFromGeoJSON(:geometry),4326))"
    )
    value = {"geometry": f"{geometry}"}
    f = f.bindparams(**value)

    return f


def geodesic_point_buffer(lat, lng, zoom):
    """Return either a point at or polygon surrounding provided latitude and
    longitude (depending on zoom level) as geoJSON see
    https://gis.stackexchange.com/questions/289044/creating-buffer-circle-x-
    kilometers-from-point-using-python."""
    buffer_distance = _get_buffer_distance(zoom)

    if buffer_distance:
        proj_wgs84 = pyproj.Proj(init="epsg:4326")
        # Azimuthal equidistant projection
        aeqd_proj = "+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0"
        project = partial(
            pyproj.transform,
            pyproj.Proj(aeqd_proj.format(lat=lat, lon=lng)),
            proj_wgs84,
        )
        buf = Point(0, 0).buffer(buffer_distance)  # distance in metres

        coord_list = transform(project, buf).exterior.coords[:]
        geojson_geometry = geoPolygon([coord_list])
    else:
        geojson_geometry = geoPoint((lat, lng))

    return geojson_geometry


def _get_buffer_distance(zoom: int) -> Optional[int]:
    # FIXME: Couldn't get the exact match to work, so setting a buffer
    # distance of 1m for zoom levels >9.
    # zoom_buffer: DefaultDict[int, Optional[int]] = defaultdict(lambda: None)
    zoom_buffer: DefaultDict[int, Optional[int]] = defaultdict(lambda: 1)
    zoom_buffer.update(
        {
            0: 10000,
            1: 5000,
            2: 2500,
            3: 1250,
            4: 600,
            5: 300,
            6: 150,
            7: 80,
            8: 40,
            9: 20,
        }
    )
    return zoom_buffer[zoom]


async def get_fields(dataset, version):
    asset = await assets.get_default_asset(dataset, version)
    return asset.fields


async def _features_response(rows) -> FeaturesResponse:
    """Serialize ORM response."""
    data = list(rows)
    return FeaturesResponse(data=data)
