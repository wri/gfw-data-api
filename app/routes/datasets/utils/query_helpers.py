import re
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, cast
from urllib.parse import unquote

from fastapi import HTTPException
from pglast import printers  # noqa
from pglast import parse_sql
from pglast.ast import (
    BoolExpr,
    FuncCall,
    RangeSubselect,
    RangeVar,
    RawStmt,
    SelectStmt,
    SQLValueFunction,
)
from pglast.ast import String as PgString
from pglast.parser import ParseError
from pglast.stream import RawStream

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

FORBIDDEN_FUNCTION_GROUPS: List[List[str]] = [
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

FORBIDDEN_FUNCTION_NAMES: Set[str] = {
    fn_name.lower() for group in FORBIDDEN_FUNCTION_GROUPS for fn_name in group
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
        if func_name_lower in FORBIDDEN_FUNCTION_NAMES:
            raise HTTPException(
                status_code=400,
                detail="Use of admin, system or private functions is not allowed.",
            )


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


def _no_forbidden_value_functions(parsed: Tuple[RawStmt]) -> None:
    value_functions = _get_function_names(SQLValueFunction, parsed)
    if value_functions:
        raise HTTPException(
            status_code=400,
            detail="Use of sql value functions is not allowed.",
        )


async def _add_geometry_filter(
    parsed_sql: Tuple[RawStmt, ...], geometry: Geometry
) -> Tuple[RawStmt, ...]:
    """Add a geometry intersection filter to the WHERE clause of a parsed SQL
    statement."""
    # Create the geometry filter as a separate parsed statement
    intersect_filter = f"SELECT WHERE ST_Intersects(geom, ST_SetSRID(ST_GeomFromGeoJSON('{geometry.json()}'),4326))"
    parsed_filter = parse_sql(intersect_filter)

    # Extract the WHERE clause from the filter statement
    filter_stmt: SelectStmt = cast(SelectStmt, parsed_filter[0].stmt)
    filter_where = filter_stmt.whereClause

    # Get the original SELECT statement
    select_stmt: SelectStmt = cast(SelectStmt, parsed_sql[0].stmt)

    # Combine WHERE clauses
    if select_stmt.whereClause:
        # If there's already a WHERE clause, combine with AND
        # boolop: 0 = AND_EXPR, 1 = OR_EXPR, 2 = NOT_EXPR
        combined_where = BoolExpr(
            boolop=0, args=(select_stmt.whereClause, filter_where)  # AND
        )
        select_stmt.whereClause = combined_where
    else:
        # No existing WHERE clause, just add the filter
        select_stmt.whereClause = filter_where

    return parsed_sql


def quote_ident(ident: str) -> str:
    # safe-ish Postgres identifier quoting
    return '"' + ident.replace('"', '""') + '"'


async def scrutinize_sql(
    dataset: str, version: str, geometry: Geometry | None, sql: str
) -> str:
    """Validate, constrain, and safely rewrite a user-supplied SQL query.

    This function parses an incoming SQL string, applies a series of
    strict validation checks to ensure the query is safe and
    well-formed, optionally injects a spatial filter,
    and then rewrites the query so that it targets a specific
    dataset/version table.

    The validations enforce that the SQL:
      - Contains exactly one statement
      - Is a SELECT query
      - Does not use a WITH clause
      - References exactly one table in the FROM clause
      - Contains no subqueries
      - Does not call forbidden SQL functions or value functions

    If any of these constraints are violated, an HTTP 400 error is raised.

    After validation, the function:
      - Preserves any table alias present in the original query
      - Optionally injects a geometry-based filter into the WHERE clause
      - Serializes the modified AST back into SQL
      - Rewrites the FROM clause to point to the fully-qualified
        dataset/version table, using proper identifier quoting when
        necessary

    This approach ensures that user-provided SQL can only operate on the
    intended table and cannot bypass security or resource constraints
    through complex SQL constructs.

    Parameters
    ----------
    dataset : str
        The dataset (schema) name to which the query should be constrained.

    version : str
        The dataset version (table) name. If it contains dots, it will be
        safely quoted as an identifier.

    geometry : Geometry | None
        Optional geometry used to inject a spatial filter into the query.
        If None, no geometry filter is applied.

    sql : str
        The user-supplied SQL query string.

    Returns
    -------
    str
        A validated and rewritten SQL query that is guaranteed to target
        only the specified dataset/version and comply with all enforced
        constraints.

    Raises
    ------
    HTTPException
        If the SQL is invalid, unsupported, or violates any safety rules.
    """

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

    # Capture alias (if any) from AST before we serialize
    select_stmt: SelectStmt = cast(SelectStmt, parsed[0].stmt)
    only_from = select_stmt.fromClause[0]
    if not isinstance(only_from, RangeVar):
        raise HTTPException(status_code=400, detail="Unexpected FROM clause structure.")

    alias_sql: str = ""
    if getattr(only_from, "alias", None):
        # RawStream on the alias node gives you e.g. `foo` or `AS foo`
        alias_raw = RawStream()(only_from.alias).strip()

        # Avoid duplicating AS if RawStream already included it
        if alias_raw.upper().startswith("AS "):
            alias_sql = " " + alias_raw
        else:
            alias_sql = " AS " + alias_raw

    # apply geometry filter (this edits the AST in-place)
    if geometry:
        parsed = await _add_geometry_filter(parsed, geometry)

    # turn AST back into SQL
    sql_out = RawStream()(parsed[0])

    # build our quoted schema.table
    if "." in version:
        from_part = f"{quote_ident(dataset)}.{quote_ident(version)}{alias_sql}"
    else:
        from_part = f"{dataset}.{version}{alias_sql}"

    sql_out = await _replace_from_clause(from_part, sql_out)

    return sql_out


async def _replace_from_clause(from_part: str, sql_in: str) -> str:
    """Replace the table reference in the SQL FROM clause with a new target.

    This function finds the first occurrence of a SQL `FROM` clause and
    replaces only the table identifier that immediately follows `FROM`,
    leaving the remainder of the query unchanged.

    The match is intentionally conservative:
      - It matches `FROM <table>` where `<table>` may be schema-qualified
        and/or double-quoted (e.g. `schema.table`, `"mySchema".table`).
      - It stops matching at common SQL clause boundaries such as
        `WHERE`, `JOIN`, `GROUP BY`, `ORDER BY`, `LIMIT`, or the end of
        the statement.
      - It does NOT consume those following clauses, ensuring they
        remain intact.

    The replacement is case-insensitive with respect to the `FROM`
    keyword and is suitable for safely rewriting SQL queries without
    accidentally modifying joins, filters, or subqueries.

    Example
    -------
    >>> sql = 'SELECT id FROM "mySchema".users WHERE active = true;'
    >>> await replace_from_clause('analytics.users_v2', sql)
    'SELECT id FROM analytics.users_v2 WHERE active = true;'

    Parameters
    ----------
    from_part : str
        The replacement table expression to insert after `FROM`
        (e.g. `"new_schema"."new_table"`, `temp_table`).

    sql_in : str
        The input SQL query.

    Returns
    -------
    str
        A new SQL string with the FROM clause target replaced.
    """
    pattern = (
        r"from\s+"
        r'[\w\."]+'
        r"(?=\s*(?:WHERE|JOIN|ON|GROUP\b|ORDER\b|LIMIT\b|OFFSET\b|FETCH\b|FOR\b|;|\)|$))"
    )

    sql_out = re.sub(
        pattern,
        f"FROM {from_part}",
        sql_in,
        flags=re.IGNORECASE,
    )
    return sql_out
