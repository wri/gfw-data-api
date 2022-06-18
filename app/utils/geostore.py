from typing import List
from uuid import UUID

from async_lru import alru_cache
from fastapi import HTTPException
from fastapi.logger import logger

from app.crud.geostore import get_gfw_geostore_from_any_dataset
from app.errors import BadResponseError, InvalidResponseError, RecordNotFoundError
from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.geostore import Geostore, GeostoreCommon
from app.settings.globals import FEATURE_CHECK_ALL_GEOSTORES
from app.utils import rw_api


@alru_cache(maxsize=128)
async def _get_gfw_geostore(geostore_id: UUID) -> GeostoreCommon:
    """Get GFW Geostore geometry."""

    try:
        geostore: Geostore = await get_gfw_geostore_from_any_dataset(geostore_id)
        geostore_common: GeostoreCommon = GeostoreCommon(
            geostore_id=geostore.gfw_geostore_id,
            geojson=geostore.gfw_geojson,
            area__ha=geostore.gfw_area__ha,
            bbox=geostore.gfw_bbox,
        )
    except (AttributeError, KeyError) as ex:
        logger.error(
            f"Response from GFW API for geostore {geostore_id} contained "
            f"incomplete data."
        )
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
        return await get_geostore_from_any_origin(geostore_id, geostore_origin)
    else:
        return await get_geostore_legacy(geostore_id, geostore_origin)


async def get_geostore_legacy(
    geostore_id: UUID, geostore_origin: GeostoreOrigin
) -> GeostoreCommon:
    """Looks for geometry in only the specified geostore."""
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
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


async def get_geostore_from_any_origin(
    geostore_id: UUID, geostore_origin: GeostoreOrigin
) -> GeostoreCommon:
    """Looks for geometry in all geostores, beginning with client's choice."""

    geostore_constructor = {
        GeostoreOrigin.gfw: _get_gfw_geostore,
        GeostoreOrigin.rw: rw_api.get_geostore,
    }

    geo_func = geostore_constructor.pop(geostore_origin)

    # If we get a geostore from any origin return it,
    # if we get all 404s return a 404,
    # if we get a mixture of errors return a 500 with explanation
    exceptions: List[Exception] = []
    try:
        return await geo_func(geostore_id)
    except RecordNotFoundError as e:
        exceptions.append(e)
    except Exception as e:
        logger.exception(e)
        exceptions.append(e)

    # Will we really ever have >2 geostore sources?
    # Preserve the possibility for now.
    for geo_func in geostore_constructor.values():
        try:
            return await geo_func(geostore_id)
        except RecordNotFoundError as e:
            exceptions.append(e)
        except Exception as e:
            logger.exception(e)
            exceptions.append(e)

    non_404s = [
        exception
        for exception in exceptions
        if not isinstance(exception, RecordNotFoundError)
    ]

    if non_404s:
        msg = (
            f"One or more errors were encountered looking for geostore "
            f"{geostore_id}. Please email info@wri.org for help."
        )
        raise HTTPException(status_code=500, detail=msg)
    else:
        raise HTTPException(status_code=404, detail=f"Geostore {geostore_id} not found")
