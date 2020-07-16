import json
from uuid import UUID

from asyncpg.exceptions import UniqueViolationError

from app.application import db
from app.errors import BadRequestError, RecordNotFoundError
from app.models.orm.user_areas import UserArea as ORMUserArea
from app.models.pydantic.geostore import Feature, Geostore, GeostoreOut


async def get_user_area_geostore(geostore_id: UUID):
    sql = f"""
        SELECT *
        FROM geostore
        WHERE gfw_geostore_id='{geostore_id}';"""

    row = await db.first(sql)

    if row is None:
        raise RecordNotFoundError(
            f"Area with gfw_geostore_id {geostore_id} does not exist"
        )

    print(row)
    # geo: Geostore = Geostore.from_orm(row)

    return row


async def get_geostore_by_version(dataset, version, geostore_id) -> GeostoreOut:
    sql = f"""
        SELECT *
        FROM ONLY "{dataset}"."{version}"
        WHERE geostore_id='{geostore_id}';"""

    row = db.first(sql)
    if row is not None:
        geo: GeostoreOut = GeostoreOut.from_orm(row)
        return geo
    else:
        raise RecordNotFoundError(
            f'Area with gfw_geostore_id {geostore_id} does not exist in "{dataset}"."{version}"'
        )


async def create_user_area(**data):
    # FIXME: Check the SRID of each feature, transform if necessary

    if len(data["features"]) != 1:
        raise BadRequestError("Please submit one and only one feature per request")

    feature = data["features"][0]
    geometry_str = json.dumps(feature["geometry"])
    feature_json = json.dumps(feature, sort_keys=True)

    # FIXME: The following could be far more elegant
    bbox = await db.scalar(
        f"""
        SELECT ARRAY[
            ST_XMin(ST_Envelope(ST_GeomFromGeoJSON('{geometry_str}')::geometry)),
            ST_YMin(ST_Envelope(ST_GeomFromGeoJSON('{geometry_str}')::geometry)),
            ST_XMax(ST_Envelope(ST_GeomFromGeoJSON('{geometry_str}')::geometry)),
            ST_YMax(ST_Envelope(ST_GeomFromGeoJSON('{geometry_str}')::geometry))
        ]::NUMERIC[];
        """
    )

    area = await db.scalar(
        f"""
        SELECT ST_Area(
            ST_GeomFromGeoJSON(
                '{geometry_str}'
            )
        )
        """
    )
    area = area / 10000

    # We could easily do this in Python but we want PostgreSQL's behavior
    # (if different) to be the source of truth.
    # geo_id = UUID(str(hashlib.md5(feature_json.encode("UTF-8")).hexdigest()))
    geo_id = await db.scalar(f"SELECT MD5('{feature_json}')::uuid;")

    try:
        user_area: ORMUserArea = await ORMUserArea.create(
            gfw_geostore_id=geo_id,
            gfw_geojson=feature_json,
            gfw_area__ha=area,
            gfw_bbox=bbox,
        )
    except UniqueViolationError:
        user_area = await get_user_area_geostore(geo_id)

    return user_area
