from uuid import UUID

from async_lru import alru_cache
from fastapi import HTTPException

from app.crud.geostore import get_geostore_from_anywhere
from app.errors import BadResponseError, InvalidResponseError
from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.geostore import Geometry, GeostoreHydrated
from app.utils import rw_api


@alru_cache(maxsize=128)
async def _get_gfw_geostore_geometry(geostore_id: UUID) -> Geometry:
    """Get GFW Geostore geometry."""

    geo: GeostoreHydrated = await get_geostore_from_anywhere(geostore_id)

    try:
        geometry = geo.gfw_geojson.features[0].geometry
    except KeyError:
        raise BadResponseError("Cannot fetch geostore geometry")

    if geometry is None:
        raise BadResponseError("Cannot fetch geostore geometry")

    return geometry


async def get_geostore_geometry(geostore_id: UUID, geostore_origin: str):
    geostore_constructor = {
        GeostoreOrigin.gfw: _get_gfw_geostore_geometry,
        GeostoreOrigin.rw: rw_api.get_geostore_geometry,
    }

    try:
        return await geostore_constructor[geostore_origin](geostore_id)
    except InvalidResponseError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except BadResponseError as e:
        raise HTTPException(status_code=400, detail=str(e))
