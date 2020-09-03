"""Run analysis on registered datasets."""
import json
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Query, Path, HTTPException
from fastapi.responses import ORJSONResponse

from app.errors import InvalidResponseError
from app.models.enum.analysis import RasterLayer
from app.utils.geostore import get_geostore_geometry

from ...models.enum.geostore import GeostoreOrigin
from ...models.pydantic.responses import Response
from app.settings.globals import RASTER_ANALYSIS_LAMBDA_NAME
from app.utils.aws import get_lambda_client

router = APIRouter()


@router.get(
    "/zonal/{geostore_id}",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Analysis"],
)
async def zonal_statistics(
    *,
    geostore_id: UUID = Path(..., title="Geostore ID"),
    geostore_origin: GeostoreOrigin = Query(GeostoreOrigin.gfw,
        title="Origin service of geostore ID"
    ),
    sum_layers: List[RasterLayer] = Query(..., alias="sum", title="Sum Layers"),
    group_by: Optional[List[RasterLayer]] = Query([], title="Group By Layers"),
    filters: Optional[List[RasterLayer]] = Query([], title="Filter Layers"),
    start_date: Optional[str] = Query(None, title="Start Date", regex="^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$",),
    end_date: Optional[str] = Query(None, title="End Date", regex="^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$",)
):
    """Calculate zonal statistics on any registered raster layers in a geostore."""
    geometry = await get_geostore_geometry(geostore_id, geostore_origin)

    payload = {
        "geometry": geometry,
        "group_by": group_by,
        "filters": filters,
        "sum": sum_layers,
        "start_date": start_date,
        "end_date": end_date
    }

    response = get_lambda_client().invoke(
        FunctionName=RASTER_ANALYSIS_LAMBDA_NAME,
        InvocationType="RequestResponse",
        Payload=bytes(json.dumps(payload), "utf-8"),
    )

    response_payload = json.loads(response['Payload'].read().decode())

    if response_payload['statusCode'] != 200:
        raise InvalidResponseError(f"Raster analysis returned status code {response_payload['statusCode']}")

    response_data = response_payload['body']['data']
    return Response(data=response_data)
