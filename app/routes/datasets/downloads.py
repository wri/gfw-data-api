"""Download dataset in different formats."""
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

from aiohttp import ClientError
from fastapi import APIRouter, Depends, HTTPException, Query

# from fastapi.openapi.models import APIKey
from fastapi.responses import RedirectResponse

# from ...authentication.api_keys import get_api_key
from ...crud.assets import get_assets_by_filter
from ...crud.versions import get_version
from ...main import logger
from ...models.enum.assets import AssetType
from ...models.enum.creation_options import Delimiters
from ...models.enum.geostore import GeostoreOrigin
from ...models.enum.pixetl import Grid
from ...models.pydantic.downloads import DownloadCSVIn, DownloadJSONIn
from ...models.pydantic.geostore import GeostoreCommon
from ...responses import CSVStreamingResponse, ORJSONStreamingResponse
from ...utils.aws import get_s3_client
from ...utils.geostore import get_geostore
from ...utils.path import split_s3_path
from .. import dataset_version_dependency
from .queries import _query_dataset_csv, _query_dataset_json

router: APIRouter = APIRouter()


@router.get(
    "/{dataset}/{version}/download/json",
    response_class=CSVStreamingResponse,
    tags=["Download"],
)
async def download_json(
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    sql: str = Query(..., description="SQL query."),
    geostore_id: Optional[UUID] = Query(None, description="Geostore ID. The geostore must represent a Polygon or MultiPolygon."),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, description="Service to search first for geostore."
    ),
    filename: str = Query("export.json", description="Name of export file."),
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented).

    Return results as downloadable JSON file. This endpoint only works
    for datasets with (geo-)database tables.
    """

    dataset, version = dataset_version

    await _check_downloadability(dataset, version)

    if geostore_id:
        geostore: Optional[GeostoreCommon] = await get_geostore(
            geostore_id, geostore_origin
        )
    else:
        geostore = None

    data: List[Dict[str, Any]] = await _query_dataset_json(
        dataset, version, sql, geostore
    )

    response = ORJSONStreamingResponse(data, filename=filename)

    response.headers["Cache-Control"] = "max-age=7200"  # 2h
    return response


@router.post(
    "/{dataset}/{version}/download/json",
    response_class=CSVStreamingResponse,
    tags=["Download"],
)
async def download_json_post(
    *,
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    request: DownloadJSONIn,
):
    """Execute a READ-ONLY SQL query on the given dataset version (if
    implemented).

    Return results as downloadable JSON file. This endpoint only works
    for datasets with (geo-)database tables.
    """

    dataset, version = dataset_version

    await _check_downloadability(dataset, version)

    # create geostore with unknowns as blank
    if request.geometry:
        geostore: Optional[GeostoreCommon] = GeostoreCommon(
            geojson=request.geometry, geostore_id=uuid4(), area__ha=0, bbox=[0, 0, 0, 0]
        )
    else:
        geostore = None

    data: List[Dict[str, Any]] = await _query_dataset_json(
        dataset, version, request.sql, geostore
    )

    response = ORJSONStreamingResponse(data, filename=request.filename)

    response.headers["Cache-Control"] = "max-age=7200"  # 2h
    return response


@router.get(
    "/{dataset}/{version}/download/csv",
    response_class=CSVStreamingResponse,
    tags=["Download"],
)
async def download_csv(
    dataset_version: Tuple[str, str] = Depends(dataset_version_dependency),
    sql: str = Query(..., description="SQL query."),
    geostore_id: Optional[UUID] = Query(None, description="Geostore ID. The geostore must represent a Polygon or MultiPolygon."),
    geostore_origin: GeostoreOrigin = Query(
        GeostoreOrigin.gfw, description="Service to search first for geostore."
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

    await _check_downloadability(dataset, version)

    if geostore_id:
        geostore: Optional[GeostoreCommon] = await get_geostore(
            geostore_id, geostore_origin
        )
    else:
        geostore = None

    data: StringIO = await _query_dataset_csv(
        dataset, version, sql, geostore, delimiter=delimiter
    )
    response = CSVStreamingResponse(iter([data.getvalue()]), filename=filename)

    response.headers["Cache-Control"] = "max-age=7200"  # 2h
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

    await _check_downloadability(dataset, version)

    geostore: Optional[GeostoreCommon] = None
    if request.geometry:
        geostore = GeostoreCommon(
            geojson=request.geometry, geostore_id=uuid4(), area__ha=0, bbox=[0, 0, 0, 0]
        )

    data: StringIO = await _query_dataset_csv(
        dataset, version, request.sql, geostore, request.delimiter
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
    # api_key: APIKey = Depends(get_api_key),
):
    """Download geotiff raster tile."""

    dataset, version = dataset_version

    await _check_downloadability(dataset, version)

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
    # api_key: APIKey = Depends(get_api_key),
):
    """Download ESRI Shapefile.

    Response will return a temporary redirect to download URL.
    """

    dataset, version = dataset_version

    await _check_downloadability(dataset, version)

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
    # api_key: APIKey = Depends(get_api_key),
):
    """Download Geopackage.

    Response will return a temporary redirect to download URL.
    """

    dataset, version = dataset_version

    await _check_downloadability(dataset, version)

    asset_url = await _get_asset_url(dataset, version, AssetType.geopackage)
    bucket, key = split_s3_path(asset_url)

    presigned_url: str = await _get_presigned_url(bucket, key)

    return RedirectResponse(url=presigned_url)


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


async def _check_downloadability(dataset, version):
    v = await get_version(dataset, version)
    if not v.is_downloadable:
        raise HTTPException(
            status_code=403, detail="This dataset is not available for download"
        )
