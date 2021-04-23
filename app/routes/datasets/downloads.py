"""Download dataset in different formats."""
from io import StringIO
from typing import Optional, Tuple, cast
from uuid import UUID

from aiohttp import ClientError
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse

from ...crud.assets import get_assets_by_filter, get_default_asset
from ...main import logger
from ...models.enum.assets import AssetType
from ...models.enum.creation_options import Delimiters
from ...models.enum.geostore import GeostoreOrigin
from ...models.enum.pixetl import Grid
from ...models.enum.queries import QueryFormat, QueryType
from ...models.orm.assets import Asset as AssetORM
from ...models.pydantic.downloads import DownloadCSVIn
from ...models.pydantic.geostore import Geometry
from ...responses import CSVStreamingResponse
from ...utils.aws import get_s3_client
from ...utils.geostore import get_geostore_geometry
from ...utils.path import split_s3_path
from .. import dataset_version_dependency
from .queries import _get_query_type, _orm_to_csv, _query_raster, _query_table

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
    delimiter: Delimiters = Query(
        Delimiters.comma, description="Delimiter to use for CSV file."
    ),
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented).

    Return results as downloadable CSV file. This endpoint only works
    for datasets with (geo-)database tables.
    """

    dataset, version = dataset_version

    if geostore_id:
        geometry: Optional[Geometry] = await get_geostore_geometry(
            geostore_id, geostore_origin
        )
    else:
        geometry = None

    data: StringIO = await _download_dataset(
        dataset, version, sql, geometry, delimiter=delimiter
    )
    response = CSVStreamingResponse(iter([data.getvalue()]), filename=filename)
    return response


@router.post(
    "/{dataset}/{version}/download/csv",
    response_class=CSVStreamingResponse,
    tags=["Download"],
)
async def download_csv_post(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    request: DownloadCSVIn,
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented).

    Return results as downloadable CSV file. This endpoint only works
    for datasets with (geo-)database tables.
    """

    dataset, version = dataset_version

    data: StringIO = await _download_dataset(
        dataset, version, request.sql, request.geometry, request.delimiter
    )

    response = CSVStreamingResponse(iter([data.getvalue()]), filename=request.filename)
    return response


@router.get(
    "/{dataset}/{version}/download/geotiff",
    response_class=RedirectResponse,
    tags=["Download"],
    status_code=307,
)
async def download_geotiff(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    grid: Grid = Query(..., description="Grid size of tile to download."),
    tile_id: str = Query(..., description="Tile ID of tile to download."),
    pixel_meaning: str = Query(..., description="Pixel meaning of tile to download."),
):
    """Download geotiff raster tile."""

    dataset, version = dataset_version

    asset_url = await _get_raster_tile_set_asset_url(
        dataset, version, grid, pixel_meaning
    )
    tile_url = asset_url.format(tile_id=tile_id)
    bucket, key = split_s3_path(tile_url)

    presigned_url = await _get_presigned_url(bucket, key)

    return RedirectResponse(url=presigned_url)


@router.get(
    "/{dataset}/{version}/download/shp",
    response_class=RedirectResponse,
    tags=["Download"],
    status_code=307,
)
async def download_shapefile(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
):
    """Download ESRI Shapefile.

    Response will return a temporary redirect to download URL.
    """

    dataset, version = dataset_version

    asset_url = await _get_asset_url(dataset, version, AssetType.shapefile)
    bucket, key = split_s3_path(asset_url)

    presigned_url = await _get_presigned_url(bucket, key)

    return RedirectResponse(url=presigned_url)


@router.get(
    "/{dataset}/{version}/download/gpkg",
    response_class=RedirectResponse,
    tags=["Download"],
    status_code=307,
)
async def download_geopackage(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
):
    """Download Geopackage.

    Response will return a temporary redirect to download URL.
    """

    dataset, version = dataset_version

    asset_url = await _get_asset_url(dataset, version, AssetType.geopackage)
    bucket, key = split_s3_path(asset_url)

    presigned_url = await _get_presigned_url(bucket, key)

    return RedirectResponse(url=presigned_url)


async def _download_dataset(
    dataset: str,
    version: str,
    sql: str,
    geometry: Optional[Geometry],
    delimiter: Delimiters = Delimiters.comma,
) -> StringIO:
    # Make sure we can query the dataset
    default_asset: AssetORM = await get_default_asset(dataset, version)
    query_type = _get_query_type(default_asset, geometry)
    if query_type == QueryType.table:
        response = await _query_table(
            dataset, version, sql, geometry, QueryFormat.csv, delimiter
        )
        return _orm_to_csv(response, delimiter=delimiter)
    elif query_type == QueryType.raster:
        geometry = cast(Geometry, geometry)
        results = await _query_raster(
            dataset, default_asset, sql, geometry, QueryFormat.csv, delimiter
        )
        return StringIO(results["data"])
    else:
        raise HTTPException(
            status_code=501,
            detail="This endpoint is not implemented for the given dataset.",
        )


async def _get_raster_tile_set_asset_url(
    dataset: str, version: str, grid: str, pixel_meaning: str
) -> str:
    assets = await get_assets_by_filter(
        dataset, version, asset_types=[AssetType.raster_tile_set]
    )

    if not assets:
        raise HTTPException(
            status_code=501,
            detail="This endpoint is not implemented for the given dataset.",
        )

    for asset in assets:
        if (
            asset.creation_options["grid"] == grid
            and asset.creation_options["pixel_meaning"] == pixel_meaning
        ):
            return asset.asset_uri

    raise HTTPException(
        status_code=404,
        detail=f"Dataset version does not have raster tile asset with grid {grid}.",
    )


async def _get_asset_url(dataset: str, version: str, asset_type: str) -> str:
    assets = await get_assets_by_filter(dataset, version, asset_types=[asset_type])

    # TODO: return a `409 - Conflict` error response and trigger asset generation in the background
    #  tell user to wait until asset finished processing and to try again later.
    if not assets:
        raise HTTPException(
            status_code=501,
            detail="This endpoint is not implemented for the given dataset.",
        )

    return assets[0].asset_uri


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
