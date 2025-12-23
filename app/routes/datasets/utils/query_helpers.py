from typing import Any, Dict, List, Tuple, cast, Optional, Iterable, Set
from urllib.parse import unquote

from fastapi import HTTPException
from pglast import printers  # noqa
from pglast import parse_sql
from pglast.ast import RangeSubselect, RawStmt, SelectStmt, FuncCall
from pglast.parser import ParseError
from pglast.stream import RawStream
from pglast.ast import String as PgString

from ....models.enum.pg_admin_functions import (
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
from ....models.enum.pg_sys_functions import (
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
from ....models.pydantic.geostore import Geometry


forbidden_function_group_list: List[List[str]] = [
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

forbidden_functions: Set[str] = {
    fn_name.lower() for group in forbidden_function_group_list for fn_name in group
}

def _has_only_one_statement(parsed: List[Dict[str, Any]]) -> None:
    if len(parsed) != 1:
        raise HTTPException(
            status_code=400, detail="Must use exactly one SQL statement."
        )


def _is_select_statement(parsed: Tuple[RawStmt]) -> None:
    if not isinstance(parsed[0].stmt, SelectStmt):
        raise HTTPException(status_code=400, detail="Must use SELECT statements only.")


def _has_no_with_clause(parsed: Tuple[RawStmt]) -> None:
    # Note this assumes we've already established the first statement is a SELECT
    select_stmt: SelectStmt = cast(SelectStmt, parsed[0].stmt)
    if getattr(select_stmt, "withClause", None) is not None:
        raise HTTPException(status_code=400, detail="Must not have WITH clause.")


def _only_one_from_table(parsed: Tuple[RawStmt]) -> None:
    # Note this assumes we've already established the first statement is a SELECT
    select_stmt: SelectStmt = cast(SelectStmt, parsed[0].stmt)
    from_clause = getattr(select_stmt, "fromClause", None)

    # Is it better to check for != 1? Or is this sufficient?
    if not from_clause or len(from_clause) > 1:
        raise HTTPException(
            status_code=400, detail="Must list exactly one table in FROM clause."
        )


def _no_subqueries(parsed: Tuple[RawStmt]) -> None:
    select_stmt: SelectStmt = cast(SelectStmt, parsed[0].stmt)

    from_clause = getattr(select_stmt, "fromClause", [])
    for fc in from_clause:
        if isinstance(fc, RangeSubselect):
            raise HTTPException(status_code=400, detail="Must not use sub queries.")


def _no_forbidden_functions(parsed: List[Dict[str, Any]]) -> None:
    function_names = _get_function_names(FuncCall, parsed)

    for function_name in function_names:
        func_name_lower = function_name.lower()
        # block functions which start with `pg_`, `PostGIS` or `_`
        if (
                func_name_lower.startswith("pg_")
                or func_name_lower.startswith("_")
                or func_name_lower.startswith("postgis")
        ):
            raise HTTPException(
                status_code=400,
                detail="Use of admin, system or private functions is not allowed.",
            )

        # Also block any other banished functions
        if func_name_lower in forbidden_functions:
            raise HTTPException(
                status_code=400,
                detail="Use of admin, system or private functions is not allowed.",
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

def _walk_ast(node: Any, visited: Optional[set] = None) -> Iterable[Any]:
    """Recursively walk a pglast AST node structure and yield every node.

    Handles AST node classes, iterables, and basic values.
    """
    if node is None:
        return

    # Use visited set to prevent infinite loops
    if visited is None:
        visited = set()

    # Use id() to track visited objects
    node_id = id(node)
    if node_id in visited:
        return
    visited.add(node_id)

    yield node

    # Handle tuples and lists (like targetList, fromClause)
    if isinstance(node, (tuple, list)):
        for item in node:
            yield from _walk_ast(item, visited)
    # Handle pglast v7 nodes which use __slots__ (a dict in v7)
    elif hasattr(node, "__slots__") and isinstance(node.__slots__, dict):
        for attr_name in node.__slots__.keys():
            attr_value = getattr(node, attr_name, None)
            yield from _walk_ast(attr_value, visited)
    # Fallback for nodes with __dict__
    elif hasattr(node, "__dict__"):
        for attr_value in node.__dict__.values():
            yield from _walk_ast(attr_value, visited)

def _get_function_names(node_type, parsed: Tuple[RawStmt]) -> List[str]:
    """Return all function names of a particular type in an AST."""
    select_stmt: SelectStmt = cast(SelectStmt, parsed[0].stmt)

    func_names: List[str] = []

    for node in _walk_ast(select_stmt):
        if isinstance(node, node_type):
            # Extract function name from funcname attribute
            funcname_list = getattr(node, "funcname", [])

            # Collect all parts of the function name (handles schema.function notation)
            func_parts: List[str] = []
            for part in funcname_list:
                txt = None
                # Try different ways to extract the string value
                if isinstance(part, str):
                    txt = part
                elif isinstance(part, PgString):
                    txt = part.sval
                elif hasattr(part, "node"):
                    # Wrapped node - try to get String from it
                    inner = part.node
                    if isinstance(inner, PgString):
                        txt = inner.sval
                    elif isinstance(inner, str):
                        txt = inner
                elif hasattr(part, "sval"):
                    txt = part.sval

                if txt:
                    func_parts.append(txt)

            # If we successfully extracted parts, use the last one (the actual function name)
            # In qualified names like "pg_catalog.pg_ls_dir", we want "pg_ls_dir"
            if func_parts:
                func_names.append(func_parts[-1])
            else:
                # Fallback: render the FuncCall and grab leading identifier
                rendered = RawStream()(node)
                candidate = rendered.split("(", 1)[0].strip()
                # Handle qualification like "pg_catalog.pg_ls_dir"
                func_name = candidate.split(".")[-1]
                if func_name:
                    func_names.append(func_name)

    return func_names

def _no_forbidden_value_functions(parsed: List[Dict[str, Any]]) -> None:
    value_functions = _get_item_value("SQLValueFunction", parsed)
    if value_functions:
        raise HTTPException(
            status_code=400,
            detail="Use of sql value functions is not allowed.",
        )


async def _add_geometry_filter(parsed_sql, geometry: Geometry):
    # make empty select statement with where clause including filter
    # this way we can later parse it as AST
    intersect_filter = f"SELECT WHERE ST_Intersects(geom, ST_SetSRID(ST_GeomFromGeoJSON('{geometry.json()}'),4326))"

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


async def scrutinize_sql(
    dataset: str, geometry: Geometry | None, sql: str, version: str
) -> str:
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

    if geometry:
        parsed = await _add_geometry_filter(parsed, geometry)

    # convert back to text
    sql = RawStream()(parsed[0])
    return sql
