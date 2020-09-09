"""Run analysis on registered datasets."""
import json
from typing import List, Optional, Dict, Any
from uuid import UUID

from fastapi import APIRouter, Query, Path
from fastapi.responses import ORJSONResponse

from app.errors import InvalidResponseError
from app.models.enum.analysis import RasterLayer
from app.utils.geostore import get_geostore_geometry

from ...models.enum.geostore import GeostoreOrigin
from ...models.pydantic.responses import Response
from ...models.pydantic.analysis import ZonalAnalysisRequestIn
from app.settings.globals import RASTER_ANALYSIS_LAMBDA_NAME
import aioboto3

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
    start_date: Optional[str] = Query(None, title="Start Date", description="Must be either year or YYYY-MM-DD date format.", regex="^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$",),
    end_date: Optional[str] = Query(None, title="End Date", description="Must be either year or YYYY-MM-DD date format.", regex="^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$",)
):
    """Calculate zonal statistics on any registered raster layers in a geostore."""
    geometry = await get_geostore_geometry(geostore_id, geostore_origin)
    return await _zonal_statics(
        geometry,
        sum_layers,
        group_by,
        filters,
        start_date,
        end_date,
    )


@router.post(
    "/zonal",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Analysis"],
)
async def zonal_statistics(
    request: ZonalAnalysisRequestIn
):
    return await _zonal_statics(
        request.geometry,
        request.sum,
        request.group_by,
        request.filters,
        request.start_date,
        request.end_date,
    )


async def _zonal_statics(
    geometry: Dict[str, Any],
    sum_layers: List[RasterLayer],
    group_by: Optional[List[RasterLayer]],
    filters: Optional[List[RasterLayer]],
    start_date: Optional[str],
    end_date: Optional[str],
):
    payload = {
        "geometry": geometry,
        "group_by": group_by,
        "filters": filters,
        "sum": sum_layers,
        "start_date": start_date,
        "end_date": end_date
    }

    async with aioboto3.client("lambda") as lambda_client:
        response = await lambda_client.invoke(
            FunctionName=RASTER_ANALYSIS_LAMBDA_NAME,
            InvocationType="RequestResponse",
            Payload=bytes(json.dumps(payload), "utf-8"),
        )

        response_payload = await response['Payload'].read()
        response_payload = json.loads(response_payload.decode())

        if response_payload['statusCode'] != 200:
            raise InvalidResponseError(f"Raster analysis returned status code {response_payload['statusCode']}")

        response_data = response_payload['body']['data']
        return Response(data=response_data)