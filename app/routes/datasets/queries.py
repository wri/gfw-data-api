"""Explore data entries for a given dataset version using standard SQL."""
import csv
import json
from contextlib import contextmanager
from io import StringIO
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import unquote
from uuid import UUID

from asyncpg import (
    InsufficientPrivilegeError,
    UndefinedColumnError,
    UndefinedFunctionError,
)
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import ORJSONResponse, Response
from pglast import printers  # noqa
from pglast import Node, parse_sql
from pglast.parser import ParseError
from pglast.printer import RawStream
from starlette.responses import StreamingResponse

from ...application import db
from ...crud import versions
from ...errors import RecordNotFoundError
from ...models.enum.geostore import GeostoreOrigin
from ...models.enum.pg_admin_functions import (
    advisory_lock_functions,
    backup_control_functions,
    collation_management_functions,
    configuration_settings_functions,
    database_object_location_functions,
    database_object_size_functions,
    generic_file_access_functions,
    index_maintenance_functions,
    recovery_control_functions,
    recovery_information_functions,
    replication_sql_functions,
    server_signaling_functions,
    snapshot_synchronization_functions,
    table_rewrite_information,
)
from ...models.enum.pg_sys_functions import (
    access_privilege_inquiry_functions,
    comment_information_functions,
    committed_transaction_information,
    control_data_functions,
    object_information_and_addressing_functions,
    schema_visibility_inquiry_functions,
    session_information_functions,
    system_catalog_information_functions,
    transaction_ids_and_snapshots,
)
from ...models.enum.queries import QueryFormat
from ...models.pydantic.responses import Response as ResponseModel
from ...responses import CSVResponse
from ...utils.geostore import get_geostore_geometry
from .. import dataset_dependency, version_dependency

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
    sql: str = Query(..., description="SQL query."),
    geostore_id: Optional[UUID] = Query(None, description="Geostore ID."),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, description="Origin service of geostore ID."
    ),
    format: QueryFormat = Query(
        QueryFormat.json, description="Output format of query."
    ),
    download: bool = Query(False, description="Download response as file."),
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
    _no_forbidden_functions(parsed)
    _no_forbidden_value_functions(parsed)

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

    try:
        response = await db.all(sql)
    except InsufficientPrivilegeError:
        raise HTTPException(
            status_code=403, detail="Not authorized to execute this query."
        )
    except UndefinedFunctionError:
        raise HTTPException(status_code=400, detail="Bad request. Unknown function.")
    except UndefinedColumnError as e:
        raise HTTPException(status_code=400, detail=f"Bad request. {str(e)}")

    return ResponseModel(data=response)


#
# async def parse_response(data: List, format: str, download: bool) -> Response:
#     if format == QueryFormat.json:
#         response: Response = ORJSONResponse(ResponseModel(data=data))
#
#     elif format == QueryFormat.csv:
#         with orm_to_csv(data) as stream:
#
#             if download:
#                 response = StreamingResponse(
#                     iter([stream.getvalue()]), media_type="text/csv"
#                 )
#                 response.headers[
#                     "Content-Disposition"
#                 ] = "attachment; filename=export.csv"
#
#             else:
#                 response = CSVResponse(stream)
#
#     else:
#         raise RuntimeError("Unknown format.")
#
#     return response


@contextmanager
def orm_to_csv(data):

    """Create a new csv file that represents generated data."""

    csv_file = StringIO()
    try:
        wr = csv.writer(csv_file, quoting=csv.QUOTE_NONNUMERIC)
        field_names = data[0].keys()
        wr.writerow(", ".join(field_names))
        for row in data:
            wr.writerow(row.values())
        csv_file.seek(0)
        yield csv_file
    finally:
        csv_file.close()


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


def _no_forbidden_functions(parsed: List[Dict[str, Any]]) -> None:
    functions = _get_item_value("FuncCall", parsed)

    forbidden_function_list = [
        configuration_settings_functions,
        server_signaling_functions,
        backup_control_functions,
        recovery_information_functions,
        recovery_control_functions,
        snapshot_synchronization_functions,
        replication_sql_functions,
        database_object_size_functions,
        database_object_location_functions,
        collation_management_functions,
        index_maintenance_functions,
        generic_file_access_functions,
        advisory_lock_functions,
        table_rewrite_information,
        session_information_functions,
        access_privilege_inquiry_functions,
        schema_visibility_inquiry_functions,
        system_catalog_information_functions,
        object_information_and_addressing_functions,
        comment_information_functions,
        transaction_ids_and_snapshots,
        committed_transaction_information,
        control_data_functions,
    ]

    for f in functions:
        function_names = f["funcname"]
        for fn in function_names:
            function_name = fn["String"]["str"]

            # block functions which start with `pg_` or `_`
            if function_name[:3] == "pg_" or function_name[:1] == "_":
                raise HTTPException(
                    status_code=400,
                    detail="Use of admin, system or private functions is not allowed.",
                )

            # Also block any other banished functions
            for forbidden_functions in forbidden_function_list:
                if function_name in forbidden_functions:
                    raise HTTPException(
                        status_code=400,
                        detail="Use of admin, system or private functions is not allowed.",
                    )


def _no_forbidden_value_functions(parsed: List[Dict[str, Any]]) -> None:
    value_functions = _get_item_value("SQLValueFunction", parsed)
    if value_functions:
        raise HTTPException(
            status_code=400,
            detail="Use of sql value functions is not allowed.",
        )


def _get_item_value(key: str, parsed: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return all functions in an AST."""
    # loop through statement recursively and yield all functions
    def walk_dict(d):
        for k, v in d.items():
            if k == key:
                yield v
            if isinstance(v, dict):
                yield from walk_dict(v)
            elif isinstance(v, list):
                for _v in v:
                    yield from walk_dict(_v)

    values: List[Dict[str, Any]] = list()
    for p in parsed:
        values += list(walk_dict(p))
    return values


async def _add_geostore_filter(parsed_sql, geostore_id: UUID, geostore_origin: str):
    geometry = await get_geostore_geometry(geostore_id, geostore_origin)

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
