"""Run analysis on registered datasets."""
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Path, Query
from fastapi.responses import ORJSONResponse

from ...models.enum.analysis import RasterLayer
from ...models.enum.geostore import GeostoreOrigin
from ...models.pydantic.analysis import ZonalAnalysisRequestIn
from ...models.pydantic.geostore import Geometry
from ...models.pydantic.responses import Response
from ...utils.geostore import get_geostore_geometry
from .. import DATE_REGEX
from ..datasets.queries import _query_raster_lambda

router = APIRouter()


@router.get(
    "/zonal/{geostore_id}",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Analysis"],
    deprecated=True,
)
async def zonal_statistics_get(
    *,
    geostore_id: UUID = Path(..., title="Geostore ID"),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, title="Origin service of geostore ID"
    ),
    sum_layers: List[RasterLayer] = Query(..., alias="sum", title="Sum Layers"),
    group_by: List[RasterLayer] = Query([], title="Group By Layers"),
    filters: List[RasterLayer] = Query([], title="Filter Layers"),
    start_date: Optional[str] = Query(
        None,
        title="Start Date",
        description="Must be either year or YYYY-MM-DD date format.",
        regex=DATE_REGEX,
    ),
    end_date: Optional[str] = Query(
        None,
        title="End Date",
        description="Must be either year or YYYY-MM-DD date format.",
        regex=DATE_REGEX,
    ),
):
    """Calculate zonal statistics on any registered raster layers in a
    geostore."""
    geometry: Geometry = await get_geostore_geometry(geostore_id, geostore_origin)
    return await _zonal_statistics(
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
    deprecated=True,
)
async def zonal_statistics_post(request: ZonalAnalysisRequestIn):
    return await _zonal_statistics(
        request.geometry,
        request.sum,
        request.group_by,
        request.filters,
        request.start_date,
        request.end_date,
    )


async def _zonal_statistics(
    geometry: Geometry,
    sum_layers: List[RasterLayer],
    group_by: List[RasterLayer],
    filters: List[RasterLayer],
    start_date: Optional[str],
    end_date: Optional[str],
):
    if filters:
        base = filters[0].value
    elif group_by:
        base = group_by[0].value
    else:
        base = "table"

    selectors = ",".join([f"sum({lyr.value})" for lyr in sum_layers])
    groups = ",".join([lyr.value for lyr in group_by])

    where_clauses = []
    for lyr in filters:
        if "umd_tree_cover_density" in lyr.value:
            where_clauses.append(f"{lyr.value[:-2]}__threshold = {lyr.value[-2:]}")
        else:
            where_clauses.append(f"{lyr.value} != 0")

    if start_date:
        where_clauses.append(_get_date_filter(start_date, ">="))

    if end_date:
        where_clauses.append(_get_date_filter(end_date, "<"))

    where = " and ".join(filters)

    query = f"select {selectors} from {base}"
    if where:
        query += f" where {where}"
    query += f" group by {groups}"

    data = await _query_raster_lambda(geometry, query)
    return Response(data=data)


def _get_date_filter(date: str, op: str):
    if len(date) == 4:
        return f"umd_tree_cover_loss__year {op} {date}"
    else:
        return f"umd_glad_alerts__date {op} {date}"
