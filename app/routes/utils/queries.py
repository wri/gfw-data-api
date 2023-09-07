import csv
from io import StringIO
from typing import Any, Dict, List, Optional, cast

from fastapi import HTTPException

from ...crud import assets
from ...models.enum.creation_options import Delimiters
from ...models.enum.queries import QueryFormat, QueryType
from ...models.orm.assets import Asset as AssetORM
from ...models.pydantic.geostore import GeostoreCommon
from ..datasets.queries import _get_query_type, _query_raster, _query_table


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
