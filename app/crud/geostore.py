import json
from typing import List
from uuid import UUID

from asyncpg.exceptions import UniqueViolationError
from geojson import Feature as geoFeature
from geojson import FeatureCollection as geoFeatureCollection
from sqlalchemy import Column, Table
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import TextClause

from app.application import db
from app.errors import BadRequestError, RecordNotFoundError
from app.models.orm.user_areas import UserArea as ORMUserArea
from app.models.pydantic.geostore import Feature, Geometry, Geostore, GeostoreHydrated

GEOSTORE_COLUMNS: List[Column] = [
    db.column("gfw_geostore_id"),
    db.column("gfw_geojson"),
    db.column("gfw_bbox"),
    db.column("gfw_area__ha"),
    db.column("created_on"),
    db.column("updated_on"),
]


async def get_geostore_from_anywhere(geostore_id: UUID) -> GeostoreHydrated:
    src_table: Table = db.table("geostore")

    where_clause: TextClause = db.text("gfw_geostore_id=:geostore_id")
    bind_vals = {"geostore_id": f"{geostore_id}"}
    where_clause = where_clause.bindparams(**bind_vals)

    sql: Select = db.select(GEOSTORE_COLUMNS).select_from(src_table).where(where_clause)

    row = await db.first(sql)

    if row is None:
        raise RecordNotFoundError(
            f"Area with gfw_geostore_id {geostore_id} does not exist"
        )

    geo: Geostore = Geostore.from_orm(row)

    return hydrate_geostore(geo)


async def get_geostore_by_version(dataset, version, geostore_id) -> GeostoreHydrated:
    src_table: Table = db.table(version)
    src_table.schema = dataset

    where_clause: TextClause = db.text("gfw_geostore_id=:geostore_id")
    bind_vals = {"geostore_id": f"{geostore_id}"}
    where_clause = where_clause.bindparams(**bind_vals)

    sql: Select = db.select(GEOSTORE_COLUMNS).select_from(src_table).where(where_clause)

    row = await db.first(sql)
    if row is None:
        raise RecordNotFoundError(
            f'Area with gfw_geostore_id {geostore_id} does not exist in "{dataset}"."{version}"'
        )

    geo: Geostore = Geostore.from_orm(row)
    return hydrate_geostore(geo)


async def create_user_area(**data) -> GeostoreHydrated:
    if len(data["features"]) != 1:
        raise BadRequestError("Please submit one and only one feature per request")

    # Sanitize the JSON by doing a round-trip with Postgres. We want the sort
    # order, whitespace, etc. to match what would be saved via other means
    # (in particular, via batch/scripts/add_gfw_fields.sh)
    geometry_str = json.dumps(data["features"][0]["geometry"])

    sql = db.text("SELECT ST_AsGeoJSON(ST_GeomFromGeoJSON(:geo)::geometry);")
    bind_vals = {"geo": geometry_str}
    sql = sql.bindparams(**bind_vals)
    sanitized_json = await db.scalar(sql)

    bbox: List[float] = await db.scalar(
        f"""
        SELECT ARRAY[
            ST_XMin(ST_Envelope(ST_GeomFromGeoJSON('{sanitized_json}')::geometry)),
            ST_YMin(ST_Envelope(ST_GeomFromGeoJSON('{sanitized_json}')::geometry)),
            ST_XMax(ST_Envelope(ST_GeomFromGeoJSON('{sanitized_json}')::geometry)),
            ST_YMax(ST_Envelope(ST_GeomFromGeoJSON('{sanitized_json}')::geometry))
        ]::NUMERIC[];
        """
    )

    area: float = await db.scalar(
        f"""
        SELECT ST_Area(
            ST_GeomFromGeoJSON(
                '{sanitized_json}'
            )
        )
        """
    )
    area = area / 10000

    # We could easily do this in Python but we want PostgreSQL's behavior
    # (if different) to be the source of truth.
    # geo_id = UUID(str(hashlib.md5(feature_json.encode("UTF-8")).hexdigest()))
    geo_id: UUID = await db.scalar(f"SELECT MD5('{sanitized_json}')::uuid;")

    try:
        user_area = await ORMUserArea.create(
            gfw_geostore_id=geo_id,
            gfw_geojson=sanitized_json,
            gfw_area__ha=area,
            gfw_bbox=bbox,
        )
        geo: Geostore = Geostore.from_orm(user_area)
        ret_val = hydrate_geostore(geo)
    except UniqueViolationError:
        ret_val = await get_geostore_from_anywhere(geo_id)

    return ret_val


def hydrate_geostore(geo: Geostore) -> GeostoreHydrated:
    geometry = Geometry.parse_raw(geo.gfw_geojson)

    feature = geoFeature(geometry=geometry.dict())
    feature_collection = wrap_feature_in_geojson(feature)

    ret_val: GeostoreHydrated = GeostoreHydrated.parse_obj(
        {
            "gfw_geostore_id": geo.gfw_geostore_id,
            "gfw_geojson": feature_collection,
            "gfw_area__ha": geo.gfw_area__ha,
            "gfw_bbox": geo.gfw_bbox,
            "created_on": geo.created_on,
            "updated_on": geo.updated_on,
        }
    )
    return ret_val


def wrap_feature_in_geojson(feature: Feature) -> geoFeatureCollection:
    return geoFeatureCollection([feature])
