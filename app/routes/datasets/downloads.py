"""Download dataset in different formats."""
import csv
from contextlib import contextmanager
from io import StringIO
from typing import Iterator, List, Optional, Tuple
from uuid import UUID

from aiohttp import ClientError
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.engine import RowProxy
from starlette.responses import RedirectResponse

from ...crud.assets import get_assets_by_filter
from ...main import logger
from ...models.enum.assets import AssetType
from ...models.enum.geostore import GeostoreOrigin
from ...models.enum.pixetl import Grid
from ...responses import CSVStreamingResponse
from ...utils.aws import get_s3_client
from ...utils.path import split_s3_path
from .. import dataset_version_dependency
from .queries import _query_dataset

router: APIRouter = APIRouter()


@router.get(
    "/{dataset}/{version}/download/csv",
    response_class=CSVStreamingResponse,
    tags=["Download"],
)
async def download_csv(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    sql: str = Query(..., description="SQL query."),
    geostore_id: Optional[UUID] = Query(None, description="Geostore ID."),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, description="Origin service of geostore ID."
    ),
    filename: str = Query("export.csv", description="Name of export file."),
    delimiter: str = Query(",", description="Delimiter to use for CSV file."),
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented).

    Return results as downloadable CSV file. This endpoint only works
    for datasets with (geo-)database tables.
    """

    dataset, version = dataset_version
    data: List[RowProxy] = await _query_dataset(
        dataset, version, sql, geostore_id, geostore_origin
    )

    with orm_to_csv(data, delimiter) as stream:
        response = CSVStreamingResponse(iter([stream.getvalue()]), filename=filename)
        return response


@router.get(
    "/{dataset}/{version}/download/geotiff",
    response_class=RedirectResponse,
    tags=["Download"],
)
async def download_geotiff(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    grid: Grid = Query(..., description="Grid size of tile to download."),
    tile_id: str = Query(..., description="Tile ID of tile to download."),
    # geostore_id: Optional[UUID] = Query(None, description="Geostore ID."),
    # geostore_origin: GeostoreOrigin = Query(
    #     GeostoreOrigin.gfw, description="Origin service of geostore ID."
    # ),
):
    """Download geotiff raster tile."""

    dataset, version = dataset_version

    asset_url = await _get_asset_url(dataset, version, grid)
    tile_url = asset_url.format(tile_id=tile_id)
    bucket, key = split_s3_path(tile_url)

    presigned_url = await _get_presigned_url(bucket, key)

    return RedirectResponse(url=presigned_url)


@contextmanager
def orm_to_csv(data: List[RowProxy], delimiter=",") -> Iterator[StringIO]:

    """Create a new csv file that represents generated data."""

    csv_file = StringIO()
    try:
        wr = csv.writer(csv_file, quoting=csv.QUOTE_NONNUMERIC, delimiter=delimiter)
        field_names = data[0].keys()
        wr.writerow(field_names)
        for row in data:
            wr.writerow(row.values())
        csv_file.seek(0)
        yield csv_file
    finally:
        csv_file.close()


async def _get_asset_url(dataset: str, version: str, grid: str) -> str:
    assets = await get_assets_by_filter(
        dataset, version, asset_types=[AssetType.raster_tile_set]
    )

    for asset in assets:
        if asset.creation_options["grid"] == grid:
            return asset.asset_uri

    raise HTTPException(
        status_code=404,
        detail=f"Dataset version does not have raster tile asset with grid {grid}.",
    )


async def _get_presigned_url(bucket, key):
    s3_client = get_s3_client()
    try:
        presigned_url = s3_client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=900
        )
    except ClientError as e:
        logger.error(e)
        raise HTTPException(
            status_code=404, detail="Requested resources does not exist."
        )
    return presigned_url
