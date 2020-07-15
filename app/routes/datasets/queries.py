"""Explore data entries for a given dataset version using standard SQL."""

from typing import Any, Dict, List
from urllib.parse import unquote

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import ORJSONResponse
from pglast import printers  # noqa
from pglast import Node, parse_sql
from pglast.parser import ParseError
from pglast.printer import RawStream

from app.application import ContextEngine, db
from app.crud import versions
from app.errors import RecordNotFoundError
from app.models.pydantic.responses import Response
from app.routes import dataset_dependency, version_dependency

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
    # geostore_id: Optional[UUID] = Query(None, title="Geostore ID")
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
