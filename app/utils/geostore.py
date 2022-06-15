from uuid import UUID

from async_lru import alru_cache
from fastapi import HTTPException
from fastapi.logger import logger

from app.crud.geostore import get_geostore_from_anywhere
from app.errors import BadResponseError, InvalidResponseError, RecordNotFoundError
from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.geostore import Geostore, GeostoreCommon
from app.settings.globals import FEATURE_CHECK_ALL_GEOSTORES
from app.utils import rw_api


@alru_cache(maxsize=128)
async def _get_gfw_geostore(geostore_id: UUID) -> GeostoreCommon:
    """Get GFW Geostore geometry."""

    try:
        geostore: Geostore = await get_geostore_from_anywhere(geostore_id)
        geostore_common: GeostoreCommon = GeostoreCommon(
            geostore_id=geostore.gfw_geostore_id,
            geojson=geostore.gfw_geojson,
            area__ha=geostore.gfw_area__ha,
            bbox=geostore.gfw_bbox,
        )
    except KeyError as ex:
        logger.exception(ex)
        raise BadResponseError("Cannot fetch geostore geometry")

    if geostore.gfw_geojson is None:
        logger.error(f"Geometry for geostore_id {geostore_id} is None")
        raise BadResponseError("Cannot fetch geostore geometry")

    return geostore_common


def check_all_geostores():
    return FEATURE_CHECK_ALL_GEOSTORES == "TRUE"


async def get_geostore(
    geostore_id: UUID, geostore_origin: GeostoreOrigin
) -> GeostoreCommon:
    if check_all_geostores():
        return await get_geostore_from_any_source(geostore_id, geostore_origin)
    else:
        return await get_geostore_legacy(geostore_id, geostore_origin)


async def get_geostore_legacy(
    geostore_id: UUID, geostore_origin: GeostoreOrigin
) -> GeostoreCommon:
    geostore_constructor = {
        GeostoreOrigin.gfw: _get_gfw_geostore,
        GeostoreOrigin.rw: rw_api.get_geostore,
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


async def get_geostore_from_any_source(
    geostore_id: UUID, geostore_origin: GeostoreOrigin
) -> GeostoreCommon:
    geostore_constructor = {
        GeostoreOrigin.gfw: _get_gfw_geostore,
        GeostoreOrigin.rw: rw_api.get_geostore,
    }

    geo_func = geostore_constructor.pop(geostore_origin)

    try:
        return await geo_func(geostore_id)
    except RecordNotFoundError:
        pass
    except Exception as e:
        logger.exception(e)

    # Will we really ever have >2 geostore sources?
    # Preserve the possibility for now.
    for geo_func in geostore_constructor.values():
        try:
            return await geo_func(geostore_id)
        except RecordNotFoundError:
            pass
        except Exception as e:
            logger.exception(e)
    raise HTTPException(status_code=404, detail=f"Geostore {geostore_id} not found")
