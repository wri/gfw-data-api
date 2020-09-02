from uuid import UUID

from fastapi import HTTPException

from app.errors import InvalidResponseError, BadResponseError
from app.models.enum.geostore import GeostoreOrigin
from app.utils import rw_api


async def get_geostore_geometry(geostore_id: UUID, geostore_origin: str):
    geostore_constructor = {
        # GeostoreOrigin.gfw: geostore.get_geostore_geometry,
        GeostoreOrigin.rw: rw_api.get_geostore_geometry
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