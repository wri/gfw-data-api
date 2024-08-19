"""Run analysis on registered datasets."""
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query
from fastapi.exceptions import HTTPException
from fastapi.logger import logger
from fastapi.openapi.models import APIKey
from fastapi.responses import ORJSONResponse

from ...authentication.api_keys import get_api_key
from ...models.enum.analysis import RasterLayer
from ...models.enum.geostore import GeostoreOrigin
from ...models.pydantic.analysis import ZonalAnalysisRequestIn
from ...models.pydantic.geostore import Geometry, GeostoreCommon
from ...models.pydantic.responses import Response
from ...settings.globals import GEOSTORE_SIZE_LIMIT_OTF
from ...utils.geostore import get_geostore
from .. import DATE_REGEX
from ..datasets.queries import _query_raster_lambda, _query_dataset_json

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
        GeostoreOrigin.gfw, title="Service to search first for geostore."
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
    api_key: APIKey = Depends(get_api_key),
):
    """Calculate zonal statistics on any registered raster layers in a
    geostore."""
    geostore: GeostoreCommon = await get_geostore(geostore_id, geostore_origin)

    if geostore.area__ha > GEOSTORE_SIZE_LIMIT_OTF:
        raise HTTPException(
            status_code=400,
            detail=f"Geostore area exceeds limit of {GEOSTORE_SIZE_LIMIT_OTF} ha for raster analysis.",
        )

    return await _zonal_statistics(
        geostore.geojson,
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
async def zonal_statistics_post(
    request: ZonalAnalysisRequestIn, api_key: APIKey = Depends(get_api_key)
):
    return await _zonal_statistics(
        request.geometry,
        request.sum,
        request.group_by,
        request.filters,
        request.start_date,
        request.end_date,
    )



@router.get(
    "/datamart/net_tree_cover_change",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Analysis"],
    deprecated=True,
)
async def zonal_statistics_get(
    *,
    iso: str = Query(..., title="ISO code"),
    adm1: Optional[int] = Query(None, title="Admin level 1 ID"),
    adm2: Optional[int] = Query(None, title="Admin level 2 ID"),
):
    select_fields = "iso, adm1, adm2, stable, loss, gain, disturb, net, change, gfw_area__ha"
    where_filter = f"iso = '${iso}'"
    level = "iso"

    if adm1 is not None:
        where_filter += f"AND adm1 = '${adm1}'"
        level = "adm1"

        if adm2 is not None:
            where_filter += f"AND adm1 = '${adm2}'"
            level = "adm2"


    results = await _query_dataset_json(
        dataset=f"umd_{level}_net_tree_cover_change_from_height",
        version="v202209",
        sql=f"SELECT ${select_fields} FROM data WHERE ${where_filter}",
    )

    return ORJSONResponse(data=results)


async def _zonal_statistics(
    geometry: Geometry,
    sum_layers: List[RasterLayer],
    group_by: List[RasterLayer],
    filters: List[RasterLayer],
    start_date: Optional[str],
    end_date: Optional[str],
):
    if geometry.type != "Polygon" and geometry.type != "MultiPolygon":
        raise HTTPException(
            status_code=400,
            detail="Geometry must be a Polygon or MultiPolygon for raster analysis",
        )

    # OTF will just not apply a base filter
    base = "data"

    selectors = ",".join([f"sum({lyr.value})" for lyr in sum_layers])
    groups = ",".join([lyr.value for lyr in group_by])

    where_clauses = []
    for lyr in filters:
        # translate ad hoc TCD layer names to actual equality
        if "umd_tree_cover_density" in lyr.value:
            where_clauses.append(f"{lyr.value[:-2]}threshold >= {lyr.value[-2:]}")
        else:
            where_clauses.append(f"{lyr.value} != 'false'")

    if start_date:
        date_filter = _get_date_filter(start_date, ">=", group_by + filters)
        if date_filter:
            where_clauses.append(date_filter)

    if end_date:
        date_filter = _get_date_filter(end_date, "<=", group_by + filters)
        if date_filter:
            where_clauses.append(date_filter)

    where = " and ".join(where_clauses)

    query = f"select {selectors} from {base}"
    if where:
        query += f" where {where}"

    if groups:
        query += f" group by {groups}"

    # replace deprecated layers
    query = query.replace(
        "umd_glad_alerts__isoweek", "isoweek(umd_glad_landsat_alerts__date)"
    )
    query = query.replace("umd_glad_alerts__date", "umd_glad_landsat_alerts__date")
    query = query.replace("sum(alert__count)", "count(*)")

    logger.info(f"Executing analysis query: {query}")

    resp = await _query_raster_lambda(geometry, query)
    data = _postprocess(resp["data"])

    # keep deprecate column names
    return Response(data=data)


def _postprocess(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for row in data:
        for col, val in list(row.items()):
            if "umd_glad_landsat_alerts" in col:
                new_col = col.replace("umd_glad_landsat_alerts", "umd_glad_alerts")
                row[new_col] = val
                del row[col]
            elif col == "count":
                row["alert__count"] = val
                del row[col]

    return data


def _get_date_filter(
    date: str, op: str, filter_layers: List[RasterLayer]
) -> Optional[str]:
    if RasterLayer.umd_tree_cover_loss__year in filter_layers:
        # only get year for TCL
        date = date if len(date) == 4 else date[:4]
        return f"umd_tree_cover_loss__year {op} {date}"
    elif RasterLayer.umd_glad_alerts__date in filter_layers:
        return f"umd_glad_landsat_alerts__date {op} '{date}'"
    else:
        # no date layer to filter by
        return None
