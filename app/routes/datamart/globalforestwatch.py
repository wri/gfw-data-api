"""APIs for Global Forest Watch data."""

from typing import Optional

from fastapi import Query, APIRouter, HTTPException
from fastapi.logger import logger
from fastapi.responses import ORJSONResponse
from httpx import AsyncClient

from app.models.pydantic.responses import Response

router = APIRouter()

PRODUCTION_SERVICE_URI = "https://data-api.globalforestwatch.org"
NET_CHANGE_VERSION = "v202209"

@router.get(
    "/v1/net_tree_cover_change",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Data Mart"],
)
async def net_tree_cover_change(
    *,
    iso: str = Query(..., title="ISO code"),
    adm1: Optional[int] = Query(None, title="Admin level 1 ID"),
    adm2: Optional[int] = Query(None, title="Admin level 2 ID"),
):
    select_fields = "iso"
    where_filter = f"iso = '{iso}'"
    level = "adm0"

    if adm1 is not None:
        where_filter += f"AND adm1 = '{adm1}'"
        select_fields += ", adm1"
        level = "adm1"

        if adm2 is not None:
            where_filter += f"AND adm2 = '{adm2}'"
            select_fields += ", adm2"
            level = "adm2"
    elif adm2 is not None:
        raise HTTPException(400, "If query for adm2, you must also include an adm1 parameter.")

    select_fields += ", stable, loss, gain, disturb, net, change, gfw_area__ha"

    async with AsyncClient() as client:
        sql = f"SELECT {select_fields} FROM data WHERE {where_filter}"
        url = f"{PRODUCTION_SERVICE_URI}/dataset/umd_{level}_net_tree_cover_change_from_height/{NET_CHANGE_VERSION}/query/json?sql={sql}"

        response = await client.get(url)
        if response.status_code != 200:
            logger.error(f"API responded with status code {response.status_code}: {response.content}")
            raise HTTPException(500, "Internal Server Error")

        results = response.json()["data"]

    return Response(data=results)
