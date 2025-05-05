import json
from typing import Dict, List, Optional, Tuple
from uuid import UUID

from asyncpg.exceptions import UniqueViolationError
from fastapi.logger import logger
from sqlalchemy import Column, Table, func
from sqlalchemy.sql import Select, label
from sqlalchemy.sql.elements import Label, TextClause

from app.application import db
from app.errors import (
    BadAdminSourceException,
    BadAdminVersionException,
    GeometryIsNullError,
    RecordNotFoundError,
)
from app.models.orm.user_areas import UserArea as ORMUserArea
from app.models.pydantic.geostore import (
    Adm0BoundaryInfo,
    Adm1BoundaryInfo,
    Adm2BoundaryInfo,
    AdminGeostore,
    AdminGeostoreResponse,
    AdminListResponse,
    Geometry,
    Geostore,
)
from app.settings.globals import ENV, per_env_admin_boundary_versions
from app.utils.gadm import extract_level_id, fix_id_pattern

GEOSTORE_COLUMNS: List[Column] = [
    db.column("gfw_geostore_id"),
    db.column("gfw_geojson"),
    db.column("gfw_bbox"),
    db.column("gfw_area__ha"),
    db.column("created_on"),
    db.column("updated_on"),
]


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

    rows = await get_all_rows(sql)

    return AdminListResponse.parse_obj(
        {
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


async def get_all_rows(sql: Select):
    rows = await db.all(sql)

    return rows


async def get_first_row(sql: Select):
    row = await db.first(sql)

    return row


async def get_gadm_geostore_id(
    admin_provider: str,
    admin_version: str,
    adm_level: int,
    country_id: str,
    region_id: str | None = None,
    subregion_id: str | None = None,
) -> str:
    src_table = await get_versioned_dataset(admin_provider, admin_version)
    columns_etc: List[Column | Label] = [
        db.column("gfw_geostore_id"),
    ]
    row = await _find_first_geostore(
        adm_level,
        admin_provider,
        admin_version,
        columns_etc,
        country_id,
        region_id,
        src_table,
        subregion_id,
    )
    return row.gfw_geostore_id


async def build_gadm_geostore(
    admin_provider: str,
    admin_version: str,
    adm_level: int,
    simplify: float | None,
    country_id: str,
    region_id: str | None = None,
    subregion_id: str | None = None,
) -> AdminGeostore:
    src_table = await get_versioned_dataset(admin_provider, admin_version)

    columns_etc: List[Column | Label] = [
        db.column("adm_level"),
        db.column("gfw_area__ha"),
        db.column("gfw_bbox"),
        db.column("gfw_geostore_id"),
        label("level_id", db.column(f"gid_{adm_level}")),
    ]

    if adm_level == 0:
        columns_etc.append(label("name", db.column("country")))
    else:
        columns_etc.append(label("name", db.column(f"name_{adm_level}")))

    if simplify is None:
        columns_etc.append(label("geojson", func.ST_AsGeoJSON(db.column("geom"))))
    else:
        columns_etc.append(
            label(
                "geojson",
                func.ST_AsGeoJSON(func.ST_Simplify(db.column("geom"), simplify)),
            )
        )

    row = await _find_first_geostore(
        adm_level,
        admin_provider,
        admin_version,
        columns_etc,
        country_id,
        region_id,
        src_table,
        subregion_id,
    )

    if row.geojson is None:
        raise GeometryIsNullError(
            "GeoJSON is None, try reducing or eliminating simplification."
        )

    return await form_admin_geostore(
        adm_level=adm_level,
        admin_version=admin_version,
        area=float(row.gfw_area__ha),
        bbox=[float(val) for val in row.gfw_bbox],
        name=str(row.name),
        geojson=json.loads(row.geojson),
        geostore_id=str(row.gfw_geostore_id),
        level_id=str(row.level_id),
        simplify=simplify,
    )


async def get_wdpa_geostore_id(dataset, version, wdpa_id):
    src_table: Table = db.table(version)
    src_table.schema = dataset
    columns_etc: List[Column | Label] = [
        db.column("gfw_geostore_id"),
    ]

    sql: Select = (
        db.select(columns_etc)
        .select_from(src_table)
        .where(db.text("wdpa_pid=:wdpa_id").bindparams(wdpa_id=wdpa_id))
    )
    row = await db.first(sql)

    if row is None:
        raise RecordNotFoundError(
            f"WDPA area with id {wdpa_id} not found in {dataset} version {version}"
        )
    return row.gfw_geostore_id


async def _find_first_geostore(
    adm_level,
    admin_provider,
    admin_version,
    columns_etc,
    country_id,
    region_id,
    src_table,
    subregion_id,
):
    sql: Select = db.select(columns_etc).select_from(src_table)
    sql = await add_where_clauses(
        adm_level,
        admin_provider,
        admin_version,
        country_id,
        region_id,
        sql,
        subregion_id,
    )
    row = await get_first_row(sql)
    if row is None:
        raise RecordNotFoundError(
            f"Admin boundary not found in {admin_provider} version {admin_version}"
        )
    return row


async def add_where_clauses(
    adm_level, admin_provider, admin_version, country_id, region_id, sql, subregion_id
):
    where_clauses: List[TextClause] = [
        db.text("adm_level=:adm_level").bindparams(adm_level=str(adm_level))
    ]
    # gid_0 is just a three-character value, but all more specific ids are
    # followed by an underscore (which has to be escaped because normally in
    # SQL an underscore is a wildcard) and a revision number (for which we
    # use an UN-escaped underscore).
    level_id_pattern: str = country_id
    if adm_level == 0:  # Special-case to avoid slow LIKE
        where_clauses.append(
            db.text("gid_0=:level_id_pattern").bindparams(
                level_id_pattern=level_id_pattern
            )
        )
    else:
        assert region_id is not None
        level_id_pattern = ".".join((level_id_pattern, region_id))
        if adm_level >= 2:
            assert subregion_id is not None
            level_id_pattern = ".".join((level_id_pattern, subregion_id))
        level_id_pattern += r"\__"

        # Adjust for any errata
        level_id_pattern = fix_id_pattern(
            adm_level, level_id_pattern, admin_provider, admin_version
        )

        where_clauses.append(
            db.text(f"gid_{adm_level} LIKE :level_id_pattern").bindparams(
                level_id_pattern=level_id_pattern
            )
        )
    for clause in where_clauses:
        sql = sql.where(clause)
    return sql


async def get_versioned_dataset(admin_provider, admin_version):
    dv: Tuple[str, str] = await admin_params_to_dataset_version(
        admin_provider, admin_version
    )
    dataset, version = dv
    src_table: Table = db.table(version)
    src_table.schema = dataset
    return src_table


async def get_gadm_geostore(
    admin_provider: str,
    admin_version: str,
    adm_level: int,
    simplify: float | None,
    country_id: str,
    region_id: str | None = None,
    subregion_id: str | None = None,
) -> AdminGeostoreResponse:
    geostore: AdminGeostore = await build_gadm_geostore(
        admin_provider=admin_provider,
        admin_version=admin_version,
        adm_level=adm_level,
        simplify=simplify,
        country_id=country_id,
        region_id=region_id,
        subregion_id=subregion_id,
    )

    return AdminGeostoreResponse(data=geostore)


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
                f" {[source.lower() for source in admin_source_to_dataset.keys()]}"
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
                f" {[v for v in per_env_admin_boundary_versions[ENV][source_provider.upper()].keys()]}"
            )
        )

    return dataset, version


async def form_admin_geostore(
    adm_level: int,
    bbox: List[float],
    area: float,
    geostore_id: str,
    level_id: str,
    simplify: Optional[float],
    admin_version: str,
    geojson: Dict,
    name: str,
) -> AdminGeostore:
    info = Adm0BoundaryInfo.parse_obj(
        {
            "use": {},
            "simplifyThresh": simplify,
            "gadm": admin_version,
            "name": name,
            "iso": extract_level_id(0, level_id),
        }
    )
    if adm_level >= 1:
        info = Adm1BoundaryInfo(
            **info.dict(),
            id1=int(extract_level_id(1, level_id)),
        )
    if adm_level == 2:
        info = Adm2BoundaryInfo(
            **info.dict(),
            id2=int(extract_level_id(2, level_id)),
        )

    return AdminGeostore.parse_obj(
        {
            "type": "geoStore",
            "id": geostore_id,
            "attributes": {
                "geojson": {
                    "crs": {},
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "geometry": geojson,
                            "properties": None,
                            "type": "Feature",
                        }
                    ],
                },
                "hash": geostore_id,
                "provider": {},
                "areaHa": area,
                "bbox": bbox,
                "lock": False,
                "info": info.dict(),
            },
        }
    )
