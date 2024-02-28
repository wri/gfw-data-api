"""Explore data entries for a given dataset version (vector and tabular data
only) in a classic RESTful way."""

from functools import partial
from typing import Any, Dict, List, Tuple

import pendulum
import pyproj
from asyncpg import UndefinedTableError
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import ORJSONResponse
from geojson import Polygon as geoPolygon
from shapely.geometry import Point
from shapely.ops import transform
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import TextClause

from ...application import db
from ...crud import assets, metadata as metadata_crud
from ...models.orm.assets import Asset as ORMAsset
from ...models.pydantic.asset_metadata import FieldMetadataOut
from ...models.pydantic.features import FeaturesResponse
from ...routes import DATE_REGEX, dataset_version_dependency, version_dependency

router = APIRouter()


def default_start():
    now = pendulum.now()
    return now.subtract(weeks=1).to_date_string()


def default_end():
    now = pendulum.now()
    return now.to_date_string()


@router.get(
    "/nasa_viirs_fire_alerts/{version}/features",
    response_class=ORJSONResponse,
    tags=["Versions"],
)
async def get_nasa_viirs_fire_alerts_features(
    *,
    version: str = Depends(version_dependency),
    lat: float = Query(..., title="Latitude", ge=-90, le=90),
    lng: float = Query(..., title="Longitude", ge=-180, le=180),
    z: int = Query(..., title="Zoom level", ge=0, le=22),
    start_date: str = Query(
        default_start(),
        regex=DATE_REGEX,
        description="Only show alerts for given date and after",
    ),
    end_date: str = Query(
        default_end(),
        regex=DATE_REGEX,
        description="Only show alerts until given date. End date cannot be in the future.",
    ),
):
    """Retrieve list of features for a given spatial location.

    Search radius various decreases for higher zoom levels.
    """
    dataset: str = "nasa_viirs_fire_alerts"
    try:
        feature_rows = await get_features_by_location_and_dates(
            dataset, version, lat, lng, z, start_date, end_date
        )
    except UndefinedTableError:
        raise HTTPException(
            status_code=501,
            detail=f"Endpoint not implemented for {dataset}.{version}."
            "Not a table or vector asset.",
        )

    return await _features_response(feature_rows)


@router.get(
    "/{dataset}/{version}/features", response_class=ORJSONResponse, tags=["Versions"]
)
async def get_features(
    *,
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
    lat: float = Query(..., title="Latitude", ge=-90, le=90),
    lng: float = Query(..., title="Longitude", ge=-180, le=180),
    z: int = Query(..., title="Zoom level", ge=0, le=22),
):
    """Retrieve list of features for a given spatial location.

    Search radius various decreases for higher zoom levels.
    """

    dataset, version = dv
    try:
        feature_rows = await get_features_by_location(dataset, version, lat, lng, z)
    except UndefinedTableError:
        raise HTTPException(
            status_code=501,
            detail=f"Endpoint not implemented for {dataset}.{version}."
            "Not a table or vector asset.",
        )

    return await _features_response(feature_rows)


async def get_features_by_location(dataset, version, lat, lng, zoom) -> List[Any]:
    sql: Select = await _get_features_by_location_sql(dataset, version, lat, lng, zoom)
    features = await db.all(sql)

    return features


async def get_features_by_location_and_dates(
    dataset: str,
    version: str,
    lat: float,
    lng: float,
    zoom: int,
    start_date: str,
    end_date: str,
) -> List[Any]:
    sql: Select = await _get_features_by_location_sql(dataset, version, lat, lng, zoom)
    sql = sql.where(date_filter("alert__date", start_date, end_date))

    features = await db.all(sql)

    return features


def date_filter(date_field: str, start_date: str, end_date: str) -> TextClause:
    f: TextClause = db.text(
        f"{date_field} BETWEEN TO_TIMESTAMP(:start_date,'YYYY-MM-DD') AND TO_TIMESTAMP(:end_date,'YYYY-MM-DD')"
    )
    values: Dict[str, Any] = {"start_date": start_date, "end_date": end_date}
    f = f.bindparams(**values)
    return f


def filter_intersects(field, geometry) -> TextClause:
    f = db.text(
        f"ST_Intersects({field}, ST_SetSRID(ST_GeomFromGeoJSON(:geometry),4326))"
    )
    value = {"geometry": f"{geometry}"}
    f = f.bindparams(**value)

    return f


def geodesic_point_buffer(lat: float, lng: float, zoom: int) -> geoPolygon:
    """Return either a point at or polygon surrounding provided latitude and
    longitude (depending on zoom level) as geoJSON see
    https://gis.stackexchange.com/questions/289044/creating-buffer-circle-x-
    kilometers-from-point-using-python."""
    buffer_distance: float = _get_buffer_distance(zoom)

    proj_wgs84 = pyproj.Proj(init="epsg:4326")
    # Azimuthal equidistant projection
    aeqd_proj = "+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0"
    project = partial(
        pyproj.transform,
        pyproj.Proj(aeqd_proj.format(lat=lat, lon=lng)),
        proj_wgs84,
    )
    buf: int = Point(0, 0).buffer(buffer_distance)  # distance in metres

    coord_list = transform(project, buf).exterior.coords[:]

    return geoPolygon([coord_list])


async def _get_fields(dataset: str, version: str) -> List[Dict[str, Any]]:
    orm_asset: ORMAsset = await assets.get_default_asset(dataset, version)
    return await metadata_crud.get_asset_fields_dicts(orm_asset)


def _get_buffer_distance(zoom: int) -> float:
    """Returns a search buffer based on the precision of the current zoom
    level."""

    # Precision of vector tiles for different zoom levels
    # https://github.com/mapbox/tippecanoe#zoom-levels
    precision: Dict[int, float] = {
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
        10: 10,
        11: 5,
        12: 2,
        13: 1,
        14: 0.5,
        15: 0.25,
        16: 0.15,
        17: 0.08,
        18: 0.04,
        19: 0.02,
        20: 0.01,
        21: 0.005,
        22: 0.0025,
    }

    # Multiplying precision by factor 50 seems to give a good balance between
    # being able to identify a feature on the map
    # and not selecting too many adjacent features at the same time
    scale_factor: int = 50

    try:
        search_buffer: float = precision[zoom] * scale_factor
    except KeyError:
        raise HTTPException(status_code=400, detail="Zoom level out of range")
    return search_buffer


async def _features_response(rows) -> FeaturesResponse:
    """Serialize ORM response."""
    data = list(rows)
    return FeaturesResponse(data=data)


async def _get_features_by_location_sql(
    dataset: str, version: str, lat: float, lng: float, zoom: int
) -> Select:
    t = db.table(version)
    t.schema = dataset

    geometry = geodesic_point_buffer(lat, lng, zoom)

    all_columns = await _get_fields(dataset, version)
    feature_columns = [
        db.column(field["name"]) for field in all_columns if field["is_feature_info"]
    ]

    sql: Select = (
        db.select(feature_columns)
        .select_from(t)
        .where(filter_intersects("geom", str(geometry)))
    )

    return sql
