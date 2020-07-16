"""Explore data entries for a given dataset version using standard SQL."""
import json
from typing import Any, Dict, List, Optional
from urllib.parse import unquote
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import ORJSONResponse
from pglast import printers  # noqa
from pglast import Node, parse_sql
from pglast.parser import ParseError
from pglast.printer import RawStream

from app.application import ContextEngine, db
from app.crud import versions
from app.errors import BadResponseError, InvalidResponseError, RecordNotFoundError
from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.responses import Response
from app.routes import dataset_dependency, version_dependency
from app.utils import rw_api

router = APIRouter()


@router.get(
    "/{dataset}/{version}/query",
    response_class=ORJSONResponse,
    response_model=Response,
    tags=["Query"],
)
async def query_dataset(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    sql: str = Query(..., title="SQL query"),
    geostore_id: Optional[UUID] = Query(None, title="Geostore ID"),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, title="Origin service of geostore ID"
    ),
):
    """Execute a read ONLY SQL query on the given dataset version (if
    implemented)."""

    # make sure version exists
    try:
        await versions.get_version(dataset, version)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=400, detail=(str(e)))

    # parse and validate SQL statement
    try:
        parsed = parse_sql(unquote(sql))
    except ParseError as e:
        raise HTTPException(status_code=400, detail=str(e))

    _has_only_one_statement(parsed)
    _is_select_statement(parsed)
    _has_no_with_clause(parsed)
    _only_one_from_table(parsed)
    _no_subqueries(parsed)

    # always overwrite the table name with the current dataset version name, to make sure no other table is queried
    parsed[0]["RawStmt"]["stmt"]["SelectStmt"]["fromClause"][0]["RangeVar"][
        "schemaname"
    ] = dataset
    parsed[0]["RawStmt"]["stmt"]["SelectStmt"]["fromClause"][0]["RangeVar"][
        "relname"
    ] = version

    if geostore_id:
        parsed = await _add_geostore_filter(parsed, geostore_id, geostore_origin)

    # convert back to text
    sql = RawStream()(Node(parsed))

    async with ContextEngine("READ"):
        response = await db.all(sql)

    return Response(data=response)


def _has_only_one_statement(parsed: List[Dict[str, Any]]) -> None:
    if len(parsed) != 1:
        raise HTTPException(
            status_code=400, detail="Must use exactly one SQL statement."
        )


def _is_select_statement(parsed: List[Dict[str, Any]]) -> None:
    select = parsed[0]["RawStmt"]["stmt"].get("SelectStmt", None)
    if not select:
        raise HTTPException(status_code=400, detail="Must use SELECT statements only.")


def _has_no_with_clause(parsed: List[Dict[str, Any]]) -> None:
    with_clause = parsed[0]["RawStmt"]["stmt"]["SelectStmt"].get("withClause", None)
    if with_clause:
        raise HTTPException(status_code=400, detail="Must not have WITH clause.")


def _only_one_from_table(parsed: List[Dict[str, Any]]) -> None:
    from_clause = parsed[0]["RawStmt"]["stmt"]["SelectStmt"].get("fromClause", None)
    if not from_clause or len(from_clause) > 1:
        raise HTTPException(
            status_code=400, detail="Must list exactly one table in FROM clause."
        )


def _no_subqueries(parsed: List[Dict[str, Any]]) -> None:
    sub_query = parsed[0]["RawStmt"]["stmt"]["SelectStmt"]["fromClause"][0].get(
        "RangeSubselect", None
    )
    if sub_query:
        raise HTTPException(status_code=400, detail="Must not use sub queries.")


async def _add_geostore_filter(parsed_sql, geostore_id: UUID, geostore_origin: str):
    geometry = await _get_geostore_geometry(geostore_id, geostore_origin)

    # make empty select statement with where clause including filter
    # this way we can later parse it as AST
    intersect_filter = f"SELECT WHERE ST_Intersects(geom, ST_SetSRID(ST_GeomFromGeoJSON('{json.dumps(geometry)}'),4326))"

    # combine the two where clauses
    parsed_filter = parse_sql(intersect_filter)
    filter_where = parsed_filter[0]["RawStmt"]["stmt"]["SelectStmt"]["whereClause"]
    sql_where = parsed_sql[0]["RawStmt"]["stmt"]["SelectStmt"].get("whereClause", None)

    if sql_where:
        parsed_sql[0]["RawStmt"]["stmt"]["SelectStmt"]["whereClause"] = {
            "BoolExpr": {"boolop": 0, "args": [sql_where, filter_where]}
        }
    else:
        parsed_sql[0]["RawStmt"]["stmt"]["SelectStmt"]["whereClause"] = filter_where

    return parsed_sql


async def _get_geostore_geometry(geostore_id: UUID, geostore_origin: str):
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
