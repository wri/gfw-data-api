"""Explore data entries for a given dataset version using standard SQL."""
import csv
import re
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple, Union, cast
from urllib.parse import unquote
from uuid import UUID, uuid4

import httpx
from asyncpg import DataError, InsufficientPrivilegeError, SyntaxOrAccessError
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import Request as FastApiRequest
from fastapi import Response as FastApiResponse
from fastapi.encoders import jsonable_encoder
from fastapi.logger import logger

# from fastapi.openapi.models import APIKey
from fastapi.responses import RedirectResponse
from pglast import printers  # noqa
from pglast import Node, parse_sql
from pglast.parser import ParseError
from pglast.printer import RawStream
from pydantic.tools import parse_obj_as
from sqlalchemy.sql import and_

from ...authentication.token import is_gfwpro_admin
from ...application import db

# from ...authentication.api_keys import get_api_key
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
from ...models.orm.queries.raster_assets import latest_raster_tile_sets
from ...models.orm.versions import Version as VersionORM
from ...models.pydantic.asset_metadata import RasterTable, RasterTableRow
from ...models.pydantic.creation_options import NoDataType
from ...models.pydantic.geostore import Geometry, GeostoreCommon
from ...models.pydantic.query import CsvQueryRequestIn, QueryRequestIn
from ...models.pydantic.raster_analysis import (
    DataEnvironment,
    DerivedLayer,
    Layer,
    SourceLayer,
)
from ...models.pydantic.responses import Response
from ...responses import CSVStreamingResponse, ORJSONLiteResponse
from ...settings.globals import GEOSTORE_SIZE_LIMIT_OTF, RASTER_ANALYSIS_LAMBDA_NAME
from ...utils.aws import invoke_lambda
from ...utils.geostore import get_geostore
from .. import dataset_version_dependency

router = APIRouter()


# Special suffixes to do an extra area density calculation on the raster data set.
AREA_DENSITY_RASTER_SUFFIXES = ["_ha-1", "_ha_yr-1"]

# Datasets that require admin privileges to do a query. (Extra protection on
# commercial datasets which shouldn't be downloaded in any way.)
PROTECTED_QUERY_DATASETS = ["wdpa_licensed_protected_areas"]

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

    This path is deprecated and will permanently redirect to /query/json.
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
    geostore_id: Optional[UUID] = Query(None, description="Geostore ID. The geostore must represent a Polygon or MultiPolygon."),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, description="Service to search first for geostore."
    ),
    # api_key: APIKey = Depends(get_api_key),
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

    A geostore ID or geometry must be specified for a query to a
    raster-only dataset.

    GET to /dataset/{dataset}/{version}/fields will show fields that can
    be used in the query. For raster-only datasets, fields for other
    raster datasets that use the same grid are listed and can be
    referenced. There are also several reserved fields with special
    meaning that can be used, including "area__ha", "latitude", and
    "longitude".

    """

    dataset, version = dataset_version
    if dataset in PROTECTED_QUERY_DATASETS:
        await is_gfwpro_admin(error_str="Unauthorized query on a restricted dataset")

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
    geostore_id: Optional[UUID] = Query(None, description="Geostore ID. The geostore must represent a Polygon or MultiPolygon."),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, description="Service to search first for geostore."
    ),
    delimiter: Delimiters = Query(
        Delimiters.comma, description="Delimiter to use for CSV file."
    ),
    # api_key: APIKey = Depends(get_api_key),
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
    # api_key: APIKey = Depends(get_api_key),
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
    # api_key: APIKey = Depends(get_api_key),
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


async def _query_dataset_json(
    dataset: str,
    version: str,
    sql: str,
    geostore: Optional[GeostoreCommon],
) -> List[Dict[str, Any]]:
    # Make sure we can query the dataset
    default_asset: AssetORM = await assets.get_default_asset(dataset, version)
    query_type = _get_query_type(default_asset, geostore)
    if query_type == QueryType.table:
        geometry = geostore.geojson if geostore else None
        return await _query_table(dataset, version, sql, geometry)
    elif query_type == QueryType.raster:
        geostore = cast(GeostoreCommon, geostore)
        results = await _query_raster(dataset, default_asset, sql, geostore)
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

    if geometry:
        parsed = await _add_geometry_filter(parsed, geometry)

    # convert back to text
    sql = RawStream()(Node(parsed))

    try:
        rows = await db.all(sql)
        response: List[Dict[str, Any]] = [dict(row) for row in rows]
    except InsufficientPrivilegeError:
        raise HTTPException(
            status_code=403, detail="Not authorized to execute this query."
        )
    except (SyntaxOrAccessError, DataError) as e:
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

            # block functions which start with `pg_`, `PostGIS` or `_`
            if (
                function_name[:3].lower() == "pg_"
                or function_name[:1].lower() == "_"
                or function_name[:7].lower() == "postgis"
            ):
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


async def _query_raster(
    dataset: str,
    asset: AssetORM,
    sql: str,
    geostore: GeostoreCommon,
    format: QueryFormat = QueryFormat.json,
    delimiter: Delimiters = Delimiters.comma,
) -> Dict[str, Any]:
    if geostore.area__ha > GEOSTORE_SIZE_LIMIT_OTF:
        raise HTTPException(
            status_code=400,
            detail=f"Geostore area exceeds limit of {GEOSTORE_SIZE_LIMIT_OTF} ha for raster analysis.",
        )
    if geostore.geojson.type != "Polygon" and geostore.geojson.type != "MultiPolygon":
        raise HTTPException(
            status_code=400,
            detail=f"Geostore must be a Polygon or MultiPolygon for raster analysis"
        )

    # use default data type to get default raster layer for dataset
    default_layer = _get_default_layer(dataset, asset.creation_options["pixel_meaning"])
    grid = asset.creation_options["grid"]
    sql = re.sub("from \w+", f"from {default_layer}", sql, flags=re.IGNORECASE)

    return await _query_raster_lambda(geostore.geojson, sql, grid, format, delimiter)


async def _query_raster_lambda(
    geometry: Geometry,
    sql: str,
    grid: Grid = Grid.ten_by_forty_thousand,
    format: QueryFormat = QueryFormat.json,
    delimiter: Delimiters = Delimiters.comma,
) -> Dict[str, Any]:
    data_environment = await _get_data_environment(grid)
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

    # invalid response codes are reserved by Lambda specific issues (e.g. too many requests)
    if response.status_code >= 300:
        raise HTTPException(
            500,
            f"Raster analysis geoprocessor returned invalid response code {response.status_code}",
        )

    # response must be in JSEND format or something unexpected happened
    response_body = response.json()
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


def _get_area_density_name(nm):
    """Return empty string if nm doesn't not have an area-density suffix, else
    return nm with the area-density suffix removed."""
    for suffix in AREA_DENSITY_RASTER_SUFFIXES:
        if nm.endswith(suffix):
            return nm[:-len(suffix)]
    return ""

def _get_default_layer(dataset, pixel_meaning):
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


async def _get_data_environment(grid: Grid) -> DataEnvironment:
    # get all raster tile set assets with the same grid.
    latest_tile_sets = await db.all(latest_raster_tile_sets, {"grid": grid})

    # build list of layers, including any derived layers, for all
    # single-band rasters found
    layers: List[Layer] = []
    for row in latest_tile_sets:
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
        if isinstance(no_data_val, List):
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
