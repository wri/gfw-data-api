import json
from typing import Any, Dict, List, Tuple
from uuid import UUID

from asyncpg.exceptions import UniqueViolationError
from fastapi.logger import logger
from sqlalchemy import Column, Table, func
from sqlalchemy.sql import Select, label
from sqlalchemy.sql.elements import TextClause

from app.application import db
from app.errors import RecordNotFoundError
from app.models.orm.user_areas import UserArea as ORMUserArea
from app.models.pydantic.geostore import (  # AdminGeostoreResponse,
    AdminListResponse,
    Geometry,
    Geostore,
)
from app.settings.globals import ENV, per_env_admin_boundary_versions

GEOSTORE_COLUMNS: List[Column] = [
    db.column("gfw_geostore_id"),
    db.column("gfw_geojson"),
    db.column("gfw_bbox"),
    db.column("gfw_area__ha"),
    db.column("created_on"),
    db.column("updated_on"),
]


class BadAdminSourceException(Exception):
    pass


class BadAdminVersionException(Exception):
    pass


async def get_gfw_geostore_from_any_dataset(geostore_id: UUID) -> Geostore:
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

    return Geostore.from_orm(row)


async def get_geostore_by_version(
    dataset: str, version: str, geostore_id: UUID
) -> Geostore:
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

    return Geostore.from_orm(row)


async def create_user_area(geometry: Geometry) -> Geostore:

    # Sanitize the JSON by doing a round-trip with Postgres. We want the sort
    # order, whitespace, etc. to match what would be saved via other means
    # (in particular, via batch/scripts/add_gfw_fields.sh)
    geometry_str = geometry.json()

    sql = db.text("SELECT ST_AsGeoJSON(ST_GeomFromGeoJSON(:geo)::geometry);")
    bind_vals = {"geo": geometry_str}
    sql = sql.bindparams(**bind_vals)
    logger.debug(sql)
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
            )::geography
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
        geostore: Geostore = Geostore.from_orm(user_area)

    except UniqueViolationError:
        geostore = await get_gfw_geostore_from_any_dataset(geo_id)

    return geostore


async def get_admin_boundary_list(
    admin_provider: str, admin_version: str
) -> AdminListResponse:
    dv: Tuple[str, str] = await admin_params_to_dataset_version(
        admin_provider, admin_version
    )
    dataset, version = dv

    src_table: Table = db.table(version)
    src_table.schema = dataset

    # What exactly is served-up by RW? It looks like it INTENDS to just
    # serve admin 0s, but the response contains much more
    where_clause: TextClause = db.text("adm_level=:adm_level").bindparams(adm_level="0")

    gadm_admin_list_columns: List[Column] = [
        db.column("adm_level"),
        db.column("gfw_geostore_id"),
        db.column("gid_0"),
        db.column("country"),
    ]
    sql: Select = (
        db.select(gadm_admin_list_columns)
        .select_from(src_table)
        .where(where_clause)
        .order_by("gid_0")
    )

    rows = await db.all(sql)

    return AdminListResponse(
        **{
            "data": [
                {
                    "geostoreId": str(row.gfw_geostore_id),
                    "iso": str(row.gid_0),
                    "name": str(row.country),
                }
                for row in rows
            ],
        }
    )


async def get_geostore_by_country_id(
    admin_provider: str, admin_version: str, country_id: str, simplify: float | None
) -> Any:
    dv: Tuple[str, str] = await admin_params_to_dataset_version(
        admin_provider, admin_version
    )
    dataset, version = dv

    src_table: Table = db.table(version)
    src_table.schema = dataset

    gadm_geostore_columns: List[Column] = [
        db.column("country"),
        db.column("gfw_bbox"),
        db.column("gfw_area__ha"),
        db.column("gfw_geostore_id"),
        db.column("gid_0"),
    ]

    if simplify is None:
        geom_expression = label("geojson", func.ST_AsGeoJSON(db.column("geom")))
    else:
        geom_expression = label(
            "geojson", func.ST_AsGeoJSON(func.ST_Simplify(db.column("geom"), simplify))
        )

    #     full_geojson_expression = func.json_build_object(
    #         'type', 'FeatureCollection',
    #         'features', func.json_agg(func.ST_AsGeoJSON("geom"))
    # )

    where_level_clause: TextClause = db.text("adm_level=:adm_level").bindparams(
        adm_level="0"
    )
    where_country_clause: TextClause = db.text("gid_0=:country_id").bindparams(
        country_id=country_id
    )

    sql: Select = (
        db.select([*gadm_geostore_columns, geom_expression])
        .select_from(src_table)
        .where(where_level_clause)
        .where(where_country_clause)
    )

    # foo = sql.compile(compile_kwargs={"literal_binds": True})
    #
    # raise Exception(f"SQL: {foo}")

    row = await db.first(sql)
    if row is None:
        raise RecordNotFoundError(
            f"Geostore with country_id {country_id} not found in GADM 4.1"  # FIXME
        )

    # return AdminGeostoreResponse(**{
    return {
        "data": {
            "type": "geoStore",
            "id": str(row.gfw_geostore_id),
            "attributes": {
                "geojson": {
                    "crs": {},
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "geometry": json.loads(row.geojson),
                            "properties": None,
                            "type": "Feature",
                        }
                    ],
                },
                "hash": str(row.gfw_geostore_id),
                "provider": {},
                "areaHa": str(row.gfw_area__ha),
                "bbox": [float(val) for val in row.gfw_bbox],
                "lock": False,
                "info": {
                    "use": {},
                    "simplifyThresh": simplify,
                    "gadm": "4.1",
                    "name": str(row.country),
                    "iso": str(row.gid_0),
                },
            },
        }
    }
    # )


async def get_geostore_by_region_id(
    admin_provider: str, admin_version: str, country_id: str, region_id: str
) -> Any:  # FIXME
    dv: Tuple[str, str] = await admin_params_to_dataset_version(
        admin_provider, admin_version
    )
    dataset, version = dv

    src_table: Table = db.table(version)
    src_table.schema = dataset

    where_level_clause: TextClause = db.text("adm_level=:adm_level").bindparams(
        adm_level="1"
    )
    where_country_clause: TextClause = db.text("gid_0=:country_id").bindparams(
        country_id=country_id
    )
    where_region_clause: TextClause = db.text("gid_1=:region_id").bindparams(
        region_id=f"{country_id}.{region_id}"
    )

    sql: Select = (
        db.select("*")
        .select_from(src_table)
        .where(where_level_clause)
        .where(where_country_clause)
        .where(where_region_clause)
    )

    # foo = (sql.compile(compile_kwargs={"literal_binds": True}))
    #
    # raise Exception(f"SQL: {foo}")

    row = await db.first(sql)
    if row is None:
        raise RecordNotFoundError(
            f"Geostore with ID {country_id}.{region_id} not found in GADM 4.1"  # FIXME
        )

    return Geostore.from_orm(row)


async def admin_params_to_dataset_version(
    source_provider: str, source_version: str
) -> Tuple[str, str]:
    admin_source_to_dataset: Dict[str, str] = {"GADM": "gadm_administrative_boundaries"}

    try:
        dataset: str = admin_source_to_dataset[source_provider.upper()]
    except KeyError:
        raise BadAdminSourceException(
            (
                "Invalid admin boundary source. Valid sources:"
                f" {[source for source in admin_source_to_dataset.keys()]}"
            )
        )

    try:
        version: str = per_env_admin_boundary_versions[ENV][source_provider.upper()][
            source_version
        ]
    except KeyError:
        raise BadAdminVersionException(
            (
                "Invalid admin boundary version. Valid versions:"
                f" {[v for v in per_env_admin_boundary_versions[ENV][source_provider].keys()]}"
            )
        )

    return dataset, version
