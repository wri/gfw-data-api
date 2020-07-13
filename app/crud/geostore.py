import hashlib
import json
import re
from uuid import UUID

from app.application import db
from app.errors import RecordNotFoundError
from app.models.orm.user_areas import UserArea as ORMUserArea
from app.models.pydantic.geostore import GeostoreOut


async def get_user_area_geostore(geostore_id: UUID) -> GeostoreOut:
    sql = f"""SELECT *
    FROM geostore
    WHERE gfw_geostore_id='{geostore_id}';"""

    row = await db.first(sql)
    print(row)
    if row is not None:
        geo: GeostoreOut = GeostoreOut.from_orm(row)
        return geo
    else:
        raise RecordNotFoundError(f"Area with geostore_id {geostore_id} does not exist")


async def get_particular_geostore(dataset, version, geostore_id) -> GeostoreOut:
    sql = f"""SELECT *
    FROM ONLY "{dataset}"."{version}"
    WHERE geostore_id='{geostore_id}';"""

    row = db.first(sql)
    if row is not None:
        geo: GeostoreOut = GeostoreOut.from_orm(row)
        return geo
    else:
        raise RecordNotFoundError(
            f'Area with geostore_id {geostore_id} does not exist in "{dataset}"."{version}"'
        )


async def create_user_area(**data):
    # FIXME: Check the SRID of each feature, transform if necessary
    if not data["features"]:
        raise Exception
    features = data["features"]
    features_string = f"""
        ST_GeomFromGeoJSON(
            '{json.dumps(features[0]["geometry"])}'
        )
    """
    for feature in features[1:]:
        features_string += [f", ST_GeomFromJSON({feature['geometry']})"]

    sql_string = f"""
        SELECT ST_SetSRID(
            ST_Envelope(
                ST_Collect(
                    {features_string}
                )
            ),
            4326
        )::Box2D;
    """
    result = await db.scalar(sql_string)
    bbox = [float(match) for match in re.findall(r"-?\d*\.\d+|-?\d+", result)]

    area = 0
    for feature in features:
        area += await db.scalar(
            f"""
            SELECT ST_Area(
                ST_GeomFromGeoJSON(
                    '{json.dumps(feature["geometry"])}'
                )
            )
        """
        )
    area = area / 10000

    sanitized_json = json.dumps(data, sort_keys=True)

    # We could easily do this in Python but we want PostgreSQL's behavior
    # to be the source of truth.
    # geo_id = UUID(str(hashlib.md5(sanitized_json.encode("UTF-8")).hexdigest()))
    geo_id = await db.scalar(
        f"""
        SELECT MD5('{sanitized_json}')::uuid;
        """
    )

    user_area: ORMUserArea = await ORMUserArea.create(
        gfw_geostore_id=geo_id,
        gfw_geojson=sanitized_json,
        gfw_area__ha=area,
        gfw_bbox=bbox,
    )

    return user_area
