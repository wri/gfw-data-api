"""Explore data entries for a given dataset version using standard SQL."""

import csv
import json
import re
import uuid
from io import StringIO
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, cast
from urllib.parse import unquote
from uuid import UUID, uuid4

import httpx
from async_lru import alru_cache
from asyncpg import DataError, InsufficientPrivilegeError, SyntaxOrAccessError
from botocore.client import BaseClient
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import Request as FastApiRequest
from fastapi import Response as FastApiResponse
from fastapi.encoders import jsonable_encoder
from fastapi.logger import logger
from fastapi.openapi.models import APIKey
from fastapi.responses import ORJSONResponse, RedirectResponse
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
from pglast.enums import BoolExprType
from pglast.parser import ParseError
from pglast.stream import RawStream
from pydantic.tools import parse_obj_as

from app.settings.globals import API_URL

from ...application import db
from ...authentication.api_keys import get_api_key
from ...authentication.token import is_gfwpro_admin_for_query
from ...crud import assets
from ...models.enum.assets import AssetType
from ...models.enum.creation_options import Delimiters
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
from ...models.enum.pixetl import Grid
from ...models.enum.queries import QueryFormat, QueryType
from ...models.orm.assets import Asset as AssetORM
from ...models.orm.queries.raster_assets import data_environment_raster_tile_sets
from ...models.pydantic.asset_metadata import RasterTable, RasterTableRow
from ...models.pydantic.creation_options import NoDataType
from ...models.pydantic.geostore import Geometry, GeostoreCommon
from ...models.pydantic.query import (
    CsvQueryRequestIn,
    QueryBatchRequestIn,
    QueryRequestIn,
)
from ...models.pydantic.raster_analysis import (
    DataEnvironment,
    DerivedLayer,
    Layer,
    SourceLayer,
)
from ...models.pydantic.responses import Response
from ...models.pydantic.user_job import UserJob, UserJobResponse
from ...responses import CSVStreamingResponse, ORJSONLiteResponse
from ...settings.globals import (
    GEOSTORE_SIZE_LIMIT_OTF,
    RASTER_ANALYSIS_LAMBDA_NAME,
    RASTER_ANALYSIS_STATE_MACHINE_ARN,
)
from ...utils.aws import get_sfn_client, invoke_lambda
from ...utils.decorators import hash_dict
from ...utils.geostore import get_geostore
from .. import dataset_version_dependency
from . import _verify_source_file_access

router = APIRouter()

# Special suffixes to do an extra area density calculation on the raster data set.
AREA_DENSITY_RASTER_SUFFIXES = ["_ha-1", "_ha_yr-1"]

# compile once at module import
_FORBIDDEN_PREFIXES_REGEX = re.compile(
    r"""\b(
        pg_[a-zA-Z0-9_]*   |   # pg_...
        _[a-zA-Z0-9_]*     |   # _private...
        postgis[a-zA-Z0-9_]*   # postgis...
    )\s*\(                  # then an opening paren, i.e. it's being called
    """,
    re.IGNORECASE | re.VERBOSE,
)

forbidden_function_list: List[List[str]] = [
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


@router.get(
    "/{dataset}/{version}/query",
    response_class=RedirectResponse,
    tags=["Query"],
    status_code=308,
    deprecated=True,
)
async def query_dataset(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    request: FastApiRequest,
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented) and return response in JSON format.

    Adding a geostore ID to the query will apply a spatial filter to the
    query, only returning results for features intersecting with the
    geostore geometry. For vector datasets, this filter will not clip
    feature geometries to the geostore boundaries. Hence any spatial
    transformation such as area calculations will be applied on the
    entire feature geometry, including areas outside the geostore
    boundaries.

    This path is deprecated and will permanently redirect to
    /query/json.
    """
    dataset, version = dataset_version
    return f"/dataset/{dataset}/{version}/query/json?{request.query_params}"


@router.get(
    "/{dataset}/{version}/query/json",
    response_model=Response,
    response_class=ORJSONLiteResponse,
    tags=["Query"],
)
async def query_dataset_json(
    response: FastApiResponse,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    sql: str = Query(..., description="SQL query."),
    geostore_id: Optional[UUID] = Query(
        None,
        description="Geostore ID. The geostore must represent a Polygon or MultiPolygon.",
    ),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, description="Service to search first for geostore."
    ),
    is_authorized: bool = Depends(is_gfwpro_admin_for_query),
    api_key: APIKey = Depends(get_api_key),
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented) and return response in JSON format.

    Adding a geostore ID or directly-specified geometry to the query
    will apply a spatial filter to the query, only returning results for
    features intersecting with the geostore geometry. For vector
    datasets, this filter will not clip feature geometries to the
    geostore boundaries. Hence any spatial transformation such as area
    calculations will be applied on the entire feature geometry,
    including areas outside the geostore boundaries.

    A geostore ID or geometry must be specified for a query to a raster-
    only dataset.

    GET to /dataset/{dataset}/{version}/fields will show fields that can
    be used in the query. For raster-only datasets, fields for other
    raster datasets that use the same grid are listed and can be
    referenced. There are also several reserved fields with special
    meaning that can be used, including "area__ha", "latitude", and
    "longitude".
    """

    dataset, version = dataset_version

    if geostore_id:
        geostore: Optional[GeostoreCommon] = await get_geostore(
            geostore_id, geostore_origin
        )
    else:
        geostore = None

    if "gadm__tcl__" in dataset:
        response.headers["Cache-Control"] = "max-age=31536000"  # 1y for TCL tables
    else:
        response.headers["Cache-Control"] = "max-age=7200"  # 2h

    json_data: List[Dict[str, Any]] = await _query_dataset_json(
        dataset, version, sql, geostore
    )
    return Response(data=json_data)


@router.get(
    "/{dataset}/{version}/query/csv",
    response_class=CSVStreamingResponse,
    tags=["Query"],
)
async def query_dataset_csv(
    response: FastApiResponse,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    sql: str = Query(..., description="SQL query."),
    geostore_id: Optional[UUID] = Query(
        None,
        description="Geostore ID. The geostore must represent a Polygon or MultiPolygon.",
    ),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, description="Service to search first for geostore."
    ),
    delimiter: Delimiters = Query(
        Delimiters.comma, description="Delimiter to use for CSV file."
    ),
    is_authorized: bool = Depends(is_gfwpro_admin_for_query),
    api_key: APIKey = Depends(get_api_key),
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented) and return response in CSV format.

    Adding a geostore ID to the query will apply a spatial filter to the
    query, only returning results for features intersecting with the
    geostore geometry. For vector datasets, this filter will not clip
    feature geometries to the geostore boundaries. Hence any spatial
    transformation such as area calculations will be applied on the
    entire feature geometry, including areas outside the geostore
    boundaries.
    """
    dataset, version = dataset_version
    if geostore_id:
        geostore: Optional[GeostoreCommon] = await get_geostore(
            geostore_id, geostore_origin
        )
    else:
        geostore = None

    response.headers["Cache-Control"] = "max-age=7200"  # 2h
    csv_data: StringIO = await _query_dataset_csv(
        dataset, version, sql, geostore, delimiter=delimiter
    )
    return CSVStreamingResponse(iter([csv_data.getvalue()]), download=False)


@router.post(
    "/{dataset}/{version}/query",
    response_class=RedirectResponse,
    status_code=308,
    tags=["Query"],
    deprecated=True,
)
async def query_dataset_post(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    request: QueryRequestIn,
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented).

    This path is deprecated and will permanently redirect to
    /query/json.
    """
    dataset, version = dataset_version
    return f"/dataset/{dataset}/{version}/query/json"


@router.post(
    "/{dataset}/{version}/query/json",
    response_class=ORJSONLiteResponse,
    response_model=Response,
    tags=["Query"],
)
async def query_dataset_json_post(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    request: QueryRequestIn,
    is_authorized: bool = Depends(is_gfwpro_admin_for_query),
    api_key: APIKey = Depends(get_api_key),
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented)."""

    dataset, version = dataset_version

    if request.geometry:
        geostore: Optional[GeostoreCommon] = GeostoreCommon(
            geojson=request.geometry, geostore_id=uuid4(), area__ha=0, bbox=[0, 0, 0, 0]
        )
    else:
        geostore = None

    json_data: List[Dict[str, Any]] = await _query_dataset_json(
        dataset, version, request.sql, geostore
    )
    return ORJSONLiteResponse(Response(data=json_data).dict())


@router.post(
    "/{dataset}/{version}/query/csv",
    response_class=CSVStreamingResponse,
    tags=["Query"],
)
async def query_dataset_csv_post(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    request: CsvQueryRequestIn,
    is_authorized: bool = Depends(is_gfwpro_admin_for_query),
    api_key: APIKey = Depends(get_api_key),
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented)."""

    dataset, version = dataset_version

    # create geostore with unknowns as blank
    if request.geometry:
        geostore: Optional[GeostoreCommon] = GeostoreCommon(
            geojson=request.geometry, geostore_id=uuid4(), area__ha=0, bbox=[0, 0, 0, 0]
        )
    else:
        geostore = None

    csv_data: StringIO = await _query_dataset_csv(
        dataset, version, request.sql, geostore, delimiter=request.delimiter
    )
    return CSVStreamingResponse(iter([csv_data.getvalue()]), download=False)


@router.post(
    "/{dataset}/{version}/query/batch",
    response_class=ORJSONResponse,
    response_model=UserJobResponse,
    tags=["Query"],
    status_code=202,
)
async def query_dataset_list_post(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    request: QueryBatchRequestIn,
    api_key: APIKey = Depends(get_api_key),
):
    """Execute a READ-ONLY SQL query on the specified raster-based dataset
    version for a potentially large list of features. The features may be
    specified by an inline GeoJson feature collection, or a list of
    ResourceWatch geostore IDs, or the URI of vector file that is in any of a
    variety of formats supported by GeoPandas, include GeoJson and CSV format.
    For CSV files, the geometry column should be named "WKT" (not "WKB") and
    the geometry values should be in WKB format.

    The specified sql query will be run on each individual feature, and
    so may take a while. Therefore, the results of this query include a
    job_id. The user should then periodically query the specified job
    via the /job/{job_id} api. When the "data.status" indicates
    "success" or "partial_success", then the successful results will be
    available at the specified "data.download_link". When the
    "data.status" indicates "partial_success" or "failed", then failed
    results (likely because of improper geometries) will be available at
    "data.failed_geometries_link". If the "data.status" indicates
    "error", then there will be no results available (nothing was able
    to complete, possible because of an infrastructure problem).

    Limitations

    - The request payload must be under 256 KB. This limit does not apply
    to features provided in a file referenced using the `uri`
    field—use this option to include larger geometry data via an external
    file.

    - There is currently a five-minute time limit on the entire list
    query, but up to 100 individual feature queries proceed in parallel,
    so lists with several thousands of features can potentially be
    processed within that time limit.
    """
    dataset, version = dataset_version

    default_asset: AssetORM = await assets.get_default_asset(dataset, version)
    if default_asset.asset_type != AssetType.raster_tile_set:
        raise HTTPException(
            status_code=400,
            detail="Querying on lists is only available for raster tile sets.",
        )

    if request.feature_collection:
        for feature in request.feature_collection.features:
            if (
                feature.geometry.type != "Polygon"
                and feature.geometry.type != "MultiPolygon"
            ):
                raise HTTPException(
                    status_code=400,
                    detail="Feature collection must only contain Polygons or MultiPolygons for raster analysis",
                )

    job_id = uuid.uuid4()

    # get grid, query and data environment based on default asset
    default_layer = _get_default_layer(
        dataset, default_asset.creation_options["pixel_meaning"]
    )
    grid = default_asset.creation_options["grid"]
    sql = re.sub("from \\w+", f"from {default_layer}", request.sql, flags=re.IGNORECASE)
    data_environment = await _get_data_environment(grid)

    input = {
        "query": sql,
        "id_field": request.id_field,
        "environment": data_environment.dict()["layers"],
    }

    if (
        (request.feature_collection and request.uri)
        or (request.feature_collection and request.geostore_ids)
        or (request.uri and request.geostore_ids)
    ):
        raise HTTPException(
            status_code=400,
            detail="Must provide only one of valid feature collection, URI, or geostore_ids list.",
        )

    if request.feature_collection is not None:
        input["feature_collection"] = jsonable_encoder(request.feature_collection)
    elif request.uri is not None:
        _verify_source_file_access([request.uri])
        input["uri"] = request.uri
    elif request.geostore_ids is not None:
        input["geostore_ids"] = request.geostore_ids
    else:
        raise HTTPException(
            status_code=400,
            detail="Must provide valid feature collection, URI, or geostore_ids list.",
        )

    try:
        sfn_client = get_sfn_client()
        await _start_batch_execution(sfn_client, job_id, input)
    except sfn_client.exceptions.ValidationException as e:
        raise HTTPException(400, f"Input failed validation. Error details: {str(e)}")
    except Exception as e:
        logger.error(e)
        return HTTPException(
            500, f"There was an error starting your job. Error details: {str(e)}"
        )

    job_link = f"{API_URL}/job/{job_id}"
    return UserJobResponse(data=UserJob(job_id=job_id, job_link=job_link))


async def _start_batch_execution(
    sfn_client: BaseClient, job_id: UUID, input: Dict[str, Any]
) -> None:
    sfn_client.start_execution(
        stateMachineArn=RASTER_ANALYSIS_STATE_MACHINE_ARN,
        name=str(job_id),
        input=json.dumps(input),
    )


async def _query_dataset_json(
    dataset: str,
    version: str,
    sql: str,
    geostore: Optional[GeostoreCommon],
    raster_version_overrides: Dict[str, str] = {},
) -> List[Dict[str, Any]]:
    # Make sure we can query the dataset
    default_asset: AssetORM = await assets.get_default_asset(dataset, version)
    query_type = _get_query_type(default_asset, geostore)
    if query_type == QueryType.table:
        geometry = geostore.geojson if geostore else None
        return await _query_table(dataset, version, sql, geometry)
    elif query_type == QueryType.raster:
        geostore = cast(GeostoreCommon, geostore)
        results = await _query_raster(
            dataset,
            default_asset,
            sql,
            geostore,
            version_overrides=raster_version_overrides,
        )
        return results["data"]
    else:
        raise HTTPException(
            status_code=501,
            detail="This endpoint is not implemented for the given dataset.",
        )


async def _query_dataset_csv(
    dataset: str,
    version: str,
    sql: str,
    geostore: Optional[GeostoreCommon],
    delimiter: Delimiters = Delimiters.comma,
) -> StringIO:
    # Make sure we can query the dataset
    default_asset: AssetORM = await assets.get_default_asset(dataset, version)
    query_type = _get_query_type(default_asset, geostore)
    if query_type == QueryType.table:
        geometry = geostore.geojson if geostore else None
        response = await _query_table(dataset, version, sql, geometry)
        return _orm_to_csv(response, delimiter=delimiter)
    elif query_type == QueryType.raster:
        geostore = cast(GeostoreCommon, geostore)
        results = await _query_raster(
            dataset, default_asset, sql, geostore, QueryFormat.csv, delimiter
        )
        return StringIO(results["data"])
    else:
        raise HTTPException(
            status_code=501,
            detail="This endpoint is not implemented for the given dataset.",
        )


def _get_query_type(default_asset: AssetORM, geostore: Optional[GeostoreCommon]):
    if default_asset.asset_type in [
        AssetType.geo_database_table,
        AssetType.database_table,
    ]:
        return QueryType.table
    elif default_asset.asset_type == AssetType.raster_tile_set:
        if not geostore:
            raise HTTPException(
                status_code=422, detail="Raster tile set queries require a geometry."
            )
        return QueryType.raster
    else:
        raise HTTPException(
            status_code=501,
            detail="This endpoint is not implemented for the given dataset.",
        )


async def _query_table(
    dataset: str,
    version: str,
    sql: str,
    geometry: Optional[Geometry],
) -> List[Dict[str, Any]]:
    # 0. Pre-parse security gate on raw SQL
    _reject_forbidden_functions_raw_sql(sql)

    # 1. Parse and validate SQL statement
    try:
        parsed: List[RawStmt] = parse_sql(unquote(sql))
    except ParseError as e:
        raise HTTPException(status_code=400, detail=str(e))

    _has_only_one_statement(parsed)
    _is_select_statement(parsed)
    _has_no_with_clause(parsed)
    _only_one_from_table(parsed)
    _no_subqueries(parsed)

    # We *still* run the AST-level forbidden function checks as a backup:
    _no_forbidden_functions(parsed)
    _no_forbidden_value_functions(parsed)

    # 2. Overwrite table name with the dataset/version we allow
    select_stmt: SelectStmt = cast(SelectStmt, parsed[0].stmt)

    only_from = select_stmt.fromClause[0]
    if isinstance(only_from, RangeVar):
        only_from.schemaname = dataset
        only_from.relname = version
    else:
        raise HTTPException(status_code=400, detail="Unexpected FROM clause structure.")

    # 3. Geometry filter if present
    if geometry:
        await _add_geometry_filter(select_stmt, geometry)

    # 4. Convert back to text
    sql_out = RawStream()(parsed[0])

    try:
        rows = await db.all(sql_out)
        response: List[Dict[str, Any]] = [dict(row) for row in rows]
    except InsufficientPrivilegeError:
        raise HTTPException(
            status_code=403,
            detail="Not authorized to execute this query.",
        )
    except (SyntaxOrAccessError, DataError) as e:
        # Keep this exactly the same string prefix that your tests are expecting
        raise HTTPException(status_code=400, detail=f"Bad request. {str(e)}")

    return response


def _orm_to_csv(
    data: List[Dict[str, Any]], delimiter: Delimiters = Delimiters.comma
) -> StringIO:
    """Create a new csv file that represents generated data.

    Response will return a temporary redirect to download URL.
    """
    csv_file = StringIO()

    if data:
        wr = csv.writer(csv_file, quoting=csv.QUOTE_NONNUMERIC, delimiter=delimiter)
        field_names = data[0].keys()
        wr.writerow(field_names)
        for row in data:
            wr.writerow(row.values())
        csv_file.seek(0)

    return csv_file


def _reject_forbidden_functions_raw_sql(sql: str) -> None:
    """Quick-and-blunt protection pass on the raw SQL text, before AST parsing.

    Blocks calls to:
      - any function starting with pg_
      - any function starting with _
      - any function starting with postgis
    Also blocks any specific function names in our forbidden lists
    regardless of prefix, even if they don't match the patterns.
    """
    # Prefix-based block (pg_*, _*, postgis*)
    if _FORBIDDEN_PREFIXES_REGEX.search(sql):
        raise HTTPException(
            status_code=400,
            detail="Use of admin, system or private functions is not allowed.",
        )

    # Flatten once for convenience
    forbidden_explicit = {
        fn_name.lower() for group in forbidden_function_list for fn_name in group
    }

    # Look for any of those explicit function names followed by '('
    # We'll match word boundary + name + optional schema qualification suffix pattern too.
    for fname in forbidden_explicit:
        # e.g. r"\bpg_reload_conf\s*\("
        pat = re.compile(r"\b" + re.escape(fname) + r"\s*\(", re.IGNORECASE)
        if pat.search(sql):
            raise HTTPException(
                status_code=400,
                detail="Use of admin, system or private functions is not allowed.",
            )


def _has_only_one_statement(parsed: List[RawStmt]) -> None:
    if len(parsed) != 1:
        raise HTTPException(
            status_code=400, detail="Must use exactly one SQL statement."
        )


def _is_select_statement(parsed: List[RawStmt]) -> None:
    if not isinstance(parsed[0].stmt, SelectStmt):
        raise HTTPException(status_code=400, detail="Must use SELECT statements only.")


def _has_no_with_clause(parsed: List[RawStmt]) -> None:
    select_stmt: SelectStmt = cast(SelectStmt, parsed[0].stmt)
    if getattr(select_stmt, "withClause", None) is not None:
        raise HTTPException(status_code=400, detail="Must not have WITH clause.")


def _only_one_from_table(parsed: List[RawStmt]) -> None:
    select_stmt: SelectStmt = cast(SelectStmt, parsed[0].stmt)
    from_clause = getattr(select_stmt, "fromClause", None)
    if not from_clause or len(from_clause) != 1:
        raise HTTPException(
            status_code=400, detail="Must list exactly one table in FROM clause."
        )


def _no_subqueries(parsed: List[RawStmt]) -> None:
    select_stmt: SelectStmt = cast(SelectStmt, parsed[0].stmt)
    from_clause = getattr(select_stmt, "fromClause", [])
    for fc in from_clause:
        if isinstance(fc, RangeSubselect):
            raise HTTPException(status_code=400, detail="Must not use sub queries.")


def _walk_ast(node: Any) -> Iterable[Any]:
    """Recursively walk a pglast AST node structure and yield every node.

    Handles AST node classes, lists, and basic values.
    """
    if node is None:
        return
    yield node

    # pglast nodes behave like dataclasses: iterate over attributes
    if hasattr(node, "__dict__"):
        for v in vars(node).values():
            for sub in _walk_ast(v):
                yield sub
    elif isinstance(node, list):
        for item in node:
            for sub in _walk_ast(item):
                yield sub


def _identifier_text_any(part: Any) -> str:
    # unwrap Node(...) if present
    if hasattr(part, "node"):
        return _identifier_text_any(part.node)

    if isinstance(part, PgString):
        sval = getattr(part, "sval", None)
        if isinstance(sval, str):
            return sval

    # fallbacks
    for attr in ("sval", "str", "val", "name"):
        v = getattr(part, attr, None)
        if isinstance(v, str) and v:
            return v

    return ""


def _no_forbidden_functions(parsed: List[RawStmt]) -> None:
    select_stmt: SelectStmt = cast(SelectStmt, parsed[0].stmt)

    # flatten + lowercase for easier testing
    forbidden_explicit = {
        fn_name.lower() for group in forbidden_function_list for fn_name in group
    }

    for node in _walk_ast(select_stmt):
        if isinstance(node, FuncCall):
            func_parts: List[str] = []

            # try to pull each identifier out of node.funcname
            for part in getattr(node, "funcname", []):
                txt = _identifier_text_any(part)
                if txt:
                    func_parts.append(txt)

            # last-resort fallback: render the FuncCall and grab leading identifier
            if not func_parts:
                rendered = RawStream()(node)
                # "postgis_full_version()" → "postgis_full_version"
                candidate = rendered.split("(", 1)[0]
                # handle qualification like "pg_catalog.pg_ls_dir"
                func_parts = [candidate.split(".")[-1]]

            for function_name in func_parts:
                low = function_name.lower()

                # prefix rules
                if (
                    low.startswith("pg_")
                    or low.startswith("_")
                    or low.startswith("postgis")
                ):
                    raise HTTPException(
                        status_code=400,
                        detail="Use of admin, system or private functions is not allowed.",
                    )

                # explicit ban list
                if low in forbidden_explicit:
                    raise HTTPException(
                        status_code=400,
                        detail="Use of admin, system or private functions is not allowed.",
                    )


def _no_forbidden_value_functions(parsed: List[RawStmt]) -> None:
    select_stmt: SelectStmt = cast(SelectStmt, parsed[0].stmt)
    for node in _walk_ast(select_stmt):
        if isinstance(node, SQLValueFunction):
            raise HTTPException(
                status_code=400,
                detail="Use of sql value functions is not allowed.",
            )


async def _add_geometry_filter(select_stmt: SelectStmt, geometry: Geometry) -> None:
    """Mutate the SelectStmt's whereClause in-place to AND an ST_Intersects
    check against the provided geometry."""
    # Generate a tiny helper SELECT with the intersects clause and
    # steal its whereClause AST. This is close to what was done previously,
    # but using node attributes instead of dict keys.
    geom_json = geometry.json()
    intersect_filter_sql = (
        "SELECT WHERE "
        f"ST_Intersects(geom, ST_SetSRID(ST_GeomFromGeoJSON('{geom_json}'),4326))"
    )

    parsed_filter: List[RawStmt] = parse_sql(intersect_filter_sql)
    filter_select: SelectStmt = cast(SelectStmt, parsed_filter[0].stmt)
    filter_where = filter_select.whereClause

    if select_stmt.whereClause:
        # Combine with AND
        select_stmt.whereClause = BoolExpr(
            boolop=BoolExprType.AND_EXPR,
            args=[select_stmt.whereClause, filter_where],
        )
    else:
        select_stmt.whereClause = filter_where


async def _query_raster(
    dataset: str,
    asset: AssetORM,
    sql: str,
    geostore: GeostoreCommon,
    format: QueryFormat = QueryFormat.json,
    delimiter: Delimiters = Delimiters.comma,
    version_overrides: Dict[str, str] = {},
) -> Dict[str, Any]:
    if geostore.area__ha > GEOSTORE_SIZE_LIMIT_OTF:
        raise HTTPException(
            status_code=400,
            detail=f"Geostore area exceeds limit of {GEOSTORE_SIZE_LIMIT_OTF} ha for raster analysis.",
        )
    if geostore.geojson.type not in ("Polygon", "MultiPolygon"):
        raise HTTPException(
            status_code=400,
            detail="Geostore must be a Polygon or MultiPolygon for raster analysis",
        )

    # use default data type to get default raster layer for dataset
    default_layer = _get_default_layer(dataset, asset.creation_options["pixel_meaning"])
    grid = asset.creation_options["grid"]
    sql = re.sub("from \\w+", f"from {default_layer}", sql, flags=re.IGNORECASE)

    return await _query_raster_lambda(
        geostore.geojson, sql, grid, format, delimiter, version_overrides
    )


async def _query_raster_lambda(
    geometry: Geometry,
    sql: str,
    grid: Grid = Grid.ten_by_forty_thousand,
    format: QueryFormat = QueryFormat.json,
    delimiter: Delimiters = Delimiters.comma,
    version_overrides: Dict[str, str] = {},
) -> Dict[str, Any]:
    data_environment = await _get_data_environment(grid, version_overrides)
    payload = {
        "geometry": jsonable_encoder(geometry),
        "query": sql,
        "environment": data_environment.dict()["layers"],
        "format": format,
    }

    logger.info(f"Submitting raster analysis lambda request with payload: {payload}")

    try:
        response = await invoke_lambda(RASTER_ANALYSIS_LAMBDA_NAME, payload)
    except httpx.TimeoutException:
        raise HTTPException(500, "Query took too long to process.")

    # invalid response codes are reserved by Lambda specific issues (e.g. throttling)
    if response.status_code >= 300:
        raise HTTPException(
            500,
            f"Raster analysis geoprocessor returned invalid response code {response.status_code}",
        )

    # response must be in JSEND format or something unexpected happened
    response_body = response.json()

    # validate JSEND-ish shape
    if "status" not in response_body or (
        "data" not in response_body and "message" not in response_body
    ):
        raise HTTPException(
            500,
            f"Raster analysis lambda received an unexpected response: {response.text}",
        )

    if response_body["status"] == "failed":
        # validation error
        raise HTTPException(422, response_body["message"])
    elif response_body["status"] == "error":
        # geoprocessing error
        raise HTTPException(500, response_body["message"])

    return response_body


def _get_area_density_name(nm: str) -> str:
    """Return '' if nm doesn't have an area-density suffix, else return nm with
    the area-density suffix removed."""
    for suffix in AREA_DENSITY_RASTER_SUFFIXES:
        if nm.endswith(suffix):
            return nm[: -len(suffix)]
    return ""


def _get_default_layer(dataset: str, pixel_meaning: str) -> str:
    default_type = pixel_meaning
    area_density_name = _get_area_density_name(default_type)
    if default_type == "is":
        return f"{default_type}__{dataset}"
    elif "date_conf" in default_type:
        # use date layer for date_conf encoding
        return f"{dataset}__date"
    elif area_density_name != "":
        # use the area_density name, in which the _ha-1 suffix (or similar) is removed.
        # OTF will multiply by pixel area to get base type
        # and table names can't include '-1'
        return f"{dataset}__{area_density_name}"
    else:
        return f"{dataset}__{default_type}"


@hash_dict
@alru_cache(maxsize=16, ttl=300.0)
async def _get_data_environment(
    grid: Grid, version_overrides: Dict[str, str] = {}
) -> DataEnvironment:
    # get all raster tile set assets with the same grid.
    sql = _get_data_environment_sql(version_overrides)
    data_environment_tile_sets = await db.all(db.text(sql), {"grid": grid})

    # build list of layers, including any derived layers, for all
    # single-band rasters found
    layers: List[Layer] = []
    for row in data_environment_tile_sets:
        creation_options = row.creation_options
        # only include single band rasters
        if creation_options.get("band_count", 1) > 1:
            continue

        if creation_options["pixel_meaning"] == "is":
            source_layer_name = f"{creation_options['pixel_meaning']}__{row['dataset']}"
        else:
            source_layer_name = f"{row['dataset']}__{creation_options['pixel_meaning']}"

        no_data_val = parse_obj_as(
            Optional[Union[List[NoDataType], NoDataType]],
            creation_options["no_data"],
        )
        if isinstance(no_data_val, list):
            no_data_val = no_data_val[0]

        raster_table = getattr(row, "values_table", None)
        layers.append(
            _get_source_layer(
                row["asset_uri"],
                source_layer_name,
                grid,
                no_data_val,
                raster_table,
            )
        )

        if creation_options["pixel_meaning"] == "date_conf":
            layers += _get_date_conf_derived_layers(source_layer_name, no_data_val)

        if _get_area_density_name(creation_options["pixel_meaning"]) != "":
            layers.append(_get_area_density_layer(source_layer_name, no_data_val))

    return DataEnvironment(layers=layers)


def _get_source_layer(
    asset_uri: str,
    source_layer_name: str,
    grid: Grid,
    no_data_val: Optional[NoDataType],
    raster_table: Optional[RasterTable],
) -> SourceLayer:
    return SourceLayer(
        source_uri=asset_uri,
        tile_scheme="nw",
        grid=grid,
        name=source_layer_name,
        no_data=no_data_val,
        raster_table=raster_table,
    )


def _get_date_conf_derived_layers(
    source_layer_name: str, no_data_val: Optional[NoDataType]
) -> List[DerivedLayer]:
    """Get derived layers that decode our date_conf layers for alert
    systems."""
    # TODO should these somehow be in the metadata or creation options instead of hardcoded?
    # 16435 is number of days from 1970-01-01 and 2015-01-01, and can be used to convet
    # our encoding of days since 2015 to a number that can be used generally for datetimes
    decode_expression = "(A + 16435).astype('datetime64[D]').astype(str)"
    encode_expression = "(datetime64(A) - 16435).astype(uint16)"
    conf_encoding = RasterTable(
        default_meaning="not_detected",
        rows=[
            RasterTableRow(value=2, meaning="nominal"),
            RasterTableRow(value=3, meaning="high"),
            RasterTableRow(value=4, meaning="highest"),
        ],
    )

    return [
        DerivedLayer(
            source_layer=source_layer_name,
            name=source_layer_name.replace("__date_conf", "__date"),
            calc="A % 10000",
            no_data=no_data_val,
            decode_expression=decode_expression,
            encode_expression=encode_expression,
        ),
        DerivedLayer(
            source_layer=source_layer_name,
            name=source_layer_name.replace("__date_conf", "__confidence"),
            calc="floor(A / 10000).astype(uint8)",
            no_data=no_data_val,
            raster_table=conf_encoding,
        ),
    ]


def _get_area_density_layer(
    source_layer_name: str, no_data_val: Optional[NoDataType]
) -> DerivedLayer:
    """Get the derived gross layer for whose values represent density per pixel
    area."""
    nm = _get_area_density_name(source_layer_name)
    return DerivedLayer(
        source_layer=source_layer_name,
        name=nm,
        calc="A * area",
        no_data=no_data_val,
    )


def _get_predefined_layers(row, source_layer_name):
    """Return predefined derived layers we use for analysis but don't actually
    exist as tile sets."""
    if source_layer_name == "whrc_aboveground_biomass_stock_2000__Mg_ha-1":
        return [
            {
                "source_layer": source_layer_name,
                "name": "whrc_aboveground_co2_emissions__Mg",
                "calc": "A * area * (0.5 * 44 / 12)",
            }
        ]


def _get_data_environment_sql(version_overrides: Dict[str, str]) -> str:
    """Construct SQL to get data environment based on version overrides.

    If no version overrides, just add condition to use latest versions.
    If version overrides provided, return those specific versions, and
    latest for the rest.
    """
    if not version_overrides:
        sql = data_environment_raster_tile_sets + " AND versions.is_latest = true"
    else:
        override_datasets = tuple(version_overrides.keys())
        override_filters = " OR ".join(
            [
                f"(assets.dataset = '{dataset}' AND assets.version = '{version}')"
                for dataset, version in version_overrides.items()
            ]
        )

        sql = (
            data_environment_raster_tile_sets
            + f" AND ((assets.dataset NOT IN {override_datasets} AND versions.is_latest = true) OR {override_filters})"
        )

    return sql
