from uuid import UUID

from async_lru import alru_cache
from fastapi import HTTPException
from fastapi.logger import logger

from app.crud.geostore import get_geostore_from_anywhere
from app.errors import BadResponseError, InvalidResponseError, RecordNotFoundError
from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.geostore import Geostore, GeostoreCommon
from app.utils import rw_api


@alru_cache(maxsize=128)
async def _get_gfw_geostore_geometry(geostore_id: UUID) -> GeostoreCommon:
    """Get GFW Geostore geometry."""

    try:
        geostore: Geostore = await get_geostore_from_anywhere(geostore_id)
        geostore_common: GeostoreCommon = GeostoreCommon(
            geostore_id=geostore.gfw_geostore_id,
            geojson=geostore.gfw_geojson,
            area__ha=geostore.gfw_area__ha,
            bbox=geostore.gfw_bbox,
        )
    except (KeyError, RecordNotFoundError) as ex:
        logger.exception(ex)
        raise BadResponseError("Cannot fetch geostore geometry")

    if geostore.gfw_geojson is None:
        logger.error(f"Geometry for geostore_id {geostore_id} is None")
        raise BadResponseError("Cannot fetch geostore geometry")

    return geostore_common


async def get_geostore_geometry(
    geostore_id: UUID, geostore_origin: str
) -> GeostoreCommon:
    geostore_constructor = {
        GeostoreOrigin.gfw: _get_gfw_geostore_geometry,
        GeostoreOrigin.rw: rw_api.get_geostore_geometry,
    }

    try:
        return await geostore_constructor[geostore_origin](geostore_id)
    except KeyError:
        raise HTTPException(
            status_code=501,
            detail=f"Geostore origin {geostore_origin} not fully implemented.",
        )
    except InvalidResponseError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except BadResponseError as e:
        raise HTTPException(status_code=400, detail=str(e))
