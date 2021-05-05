"""Explore data entries for a given dataset version using standard SQL."""
import csv
import re
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple, cast
from urllib.parse import unquote
from uuid import UUID

import httpx
from asyncpg import (
    InsufficientPrivilegeError,
    InvalidTextRepresentationError,
    UndefinedColumnError,
    UndefinedFunctionError,
)
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import Response as FastApiResponse
from fastapi.encoders import jsonable_encoder
from fastapi.logger import logger
from pglast import printers  # noqa
from pglast import Node, parse_sql
from pglast.parser import ParseError
from pglast.printer import RawStream
from sqlalchemy.sql import and_

from ...application import db
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
from ...models.enum.queries import QueryFormat, QueryType
from ...models.orm.assets import Asset as AssetORM
from ...models.orm.versions import Version as VersionORM
from ...models.pydantic.geostore import Geometry
from ...models.pydantic.query import QueryRequestIn
from ...models.pydantic.responses import Response
from ...responses import ORJSONLiteResponse
from ...settings.globals import RASTER_ANALYSIS_LAMBDA_NAME
from ...utils.aws import invoke_lambda
from ...utils.geostore import get_geostore_geometry
from .. import dataset_version_dependency

router = APIRouter()


@router.get(
    "/{dataset}/{version}/query",
    response_class=ORJSONLiteResponse,
    response_model=Response,
    tags=["Query"],
)
async def query_dataset(
    response: FastApiResponse,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    sql: str = Query(..., description="SQL query."),
    geostore_id: Optional[UUID] = Query(None, description="Geostore ID."),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, description="Origin service of geostore ID."
    ),
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented)."""

    dataset, version = dataset_version
    if geostore_id:
        geometry: Optional[Geometry] = await get_geostore_geometry(
            geostore_id, geostore_origin
        )
    else:
        geometry = None

    data: List[Dict[str, Any]] = await _query_dataset(dataset, version, sql, geometry)

    response.headers["Cache-Control"] = "max-age=7200"  # 2h
    return Response(data=data)


@router.post(
    "/{dataset}/{version}/query",
    response_class=ORJSONLiteResponse,
    response_model=Response,
    tags=["Query"],
)
async def query_dataset_post(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    request: QueryRequestIn,
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented)."""

    dataset, version = dataset_version

    data = await _query_dataset(dataset, version, request.sql, request.geometry)
    return Response(data=data)


async def _query_dataset(
    dataset: str,
    version: str,
    sql: str,
    geometry: Optional[Geometry],
) -> List[Dict[str, Any]]:
    # Make sure we can query the dataset
    default_asset: AssetORM = await assets.get_default_asset(dataset, version)
    query_type = _get_query_type(default_asset, geometry)
    if query_type == QueryType.table:
        return await _query_table(dataset, version, sql, geometry, QueryFormat.json)
    elif query_type == QueryType.raster:
        geometry = cast(Geometry, geometry)
        results = await _query_raster(dataset, default_asset, sql, geometry)
        return results["data"]
    else:
        raise HTTPException(
            status_code=501,
            detail="This endpoint is not implemented for the given dataset.",
        )


def _get_query_type(default_asset: AssetORM, geometry: Optional[Geometry]):
    if default_asset.asset_type in [
        AssetType.geo_database_table,
        AssetType.database_table,
    ]:
        return QueryType.table
    elif default_asset.asset_type == AssetType.raster_tile_set:
        if not geometry:
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
    format: QueryFormat = QueryFormat.json,
    delimiter: Delimiters = Delimiters.comma,
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
    except UndefinedFunctionError:
        raise HTTPException(status_code=400, detail="Bad request. Unknown function.")
    except (UndefinedColumnError, InvalidTextRepresentationError) as e:
        raise HTTPException(status_code=400, detail=f"Bad request. {str(e)}")

    return response


def _orm_to_csv(
    data: List[Dict[str, Any]], delimiter: Delimiters = Delimiters.comma
) -> StringIO:
    """Create a new csv file that represents generated data.

    Response will return a temporary redirect to download URL.
    """
    csv_file = StringIO()

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
    geometry: Geometry,
    format: QueryFormat = QueryFormat.json,
    delimiter: Delimiters = Delimiters.comma,
) -> Dict[str, Any]:
    # use default data type to get default raster layer for dataset
    default_type = asset.creation_options["pixel_meaning"]
    default_layer = (
        f"{default_type}__{dataset}"
        if default_type == "is"
        else f"{dataset}__{default_type}"
    )
    sql = re.sub("from \w+", f"from {default_layer}", sql.lower())
    return await _query_raster_lambda(geometry, sql, format, delimiter)


async def _query_raster_lambda(
    geometry: Geometry,
    sql: str,
    format: QueryFormat = QueryFormat.json,
    delimiter: Delimiters = Delimiters.comma,
) -> Dict[str, Any]:
    data_environment = await _get_data_environment()
    payload = {
        "geometry": jsonable_encoder(geometry),
        "query": sql,
        "environment": data_environment,
        "format": format,
    }

    logger.info(f"Submitting raster analysis lambda request with payload: {payload}")

    try:
        response = await invoke_lambda(RASTER_ANALYSIS_LAMBDA_NAME, payload)
    except httpx.TimeoutException:
        raise HTTPException(500, "Query took too long to process.")

    response_data = response.json()["body"]
    if response_data["status"] != "success":
        logger.error(
            f"Raster analysis lambda experienced an error. Full response: {response.text}"
        )
        raise HTTPException(
            500, "Raster analysis geoprocessor experienced an error. See logs."
        )

    return response_data


async def _get_data_environment():
    # get all Raster tile set assets
    latest_tile_sets = await (
        AssetORM.join(VersionORM)
        .select()
        .with_only_columns(
            [
                AssetORM.dataset,
                AssetORM.version,
                AssetORM.creation_options,
                AssetORM.asset_uri,
                AssetORM.metadata,
                VersionORM.is_latest,
            ]
        )
        .where(
            and_(
                AssetORM.asset_type == AssetType.raster_tile_set.value,
                VersionORM.is_latest == True,  # noqa: E712
            )
        )
    ).gino.all()

    # create layers
    layers = []
    for row in latest_tile_sets:
        source_layer_name = f"{row.dataset}__{row.creation_options['pixel_meaning']}"
        layers.append(_get_source_layer(row, source_layer_name))

        if row.creation_options["pixel_meaning"] == "date_conf":
            layers += _get_date_conf_derived_layers(row, source_layer_name)

        if row.creation_options["pixel_meaning"].endswith("_ha-1"):
            layers.append(_get_area_density_layer(row, source_layer_name))

    return layers


def _get_source_layer(row, source_layer_name: str):
    return {
        "source_uri": row.asset_uri,
        "tile_scheme": "nw",
        "grid": row.creation_options["grid"],
        "name": source_layer_name,
        "raster_table": row.metadata.get("raster_table", {}),
    }


def _get_date_conf_derived_layers(row, source_layer_name):
    """Get derived layers that decode our date_conf layers for alert
    systems."""
    # TODO should these somehow be in the metadata or creation options instead of hardcoded?
    # 16435 is number of days from 1970-01-01 and 2015-01-01, and can be used to convet
    # our encoding of days since 2015 to a number that can be used generally for datetimes
    decode_expression = "(A + 16435).astype('datetime64[D]').astype(str)"
    encode_expression = "(datetime64(A) - 16435).astype(uint16)"

    return [
        {
            "source_layer": source_layer_name,
            "name": source_layer_name.replace("__date_conf", "__date"),
            "calc": "A % 10000",
            "decode_expression": encode_expression,
            "encode_expression": decode_expression,
        },
        {
            "source_layer": source_layer_name,
            "name": source_layer_name.replace("__date_conf", "__confidence"),
            "calc": "floor(A / 10000)",
            "pixel_encoding": {2: "", 3: "high"},
        },
    ]


def _get_area_density_layer(row, source_layer_name):
    """Get the derived gross layer for whose values represent density per pixel
    area."""
    return {
        "source_layer": source_layer_name,
        "name": source_layer_name.replace("_ha-1", ""),
        "calc": "A * area",
    }


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
