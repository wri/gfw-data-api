from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from fastapi.responses import ORJSONResponse

from ..crud import assets
from ..models.orm.asset import Asset as ORMAsset
from ..models.pydantic.asset import Asset, AssetCreateIn, AssetType
from ..models.pydantic.change_log import ChangeLog
from ..routes import dataset_dependency, is_admin, version_dependency

router = APIRouter()

# TODO:
#  - Assets should have config parameters to allow specifying creation options
#  -- might be good to have different endpoints for different asset types to be able to validate config params?


@router.get(
    "/{dataset}/{version}/assets",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=List[Asset],
)
async def get_assets(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    asset_type: Optional[AssetType] = Query(None, title="Filter by Asset Type"),
):
    """
    Get all assets for a given dataset version
    """
    rows: List[ORMAsset] = await assets.get_assets(dataset, version)

    # Filter rows by asset type
    result = list()
    if asset_type:
        for row in rows:
            if row.asset_type == asset_type:
                result.append(row)
    else:
        result = rows

    return result


@router.get(
    "/{dataset}/{version}/assets/{asset_id}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=Asset,
)
async def get_asset(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    asset_id: UUID = Path(...),
):
    """
    Get a specific asset
    """
    row: ORMAsset = await assets.get_asset(asset_id)

    if row.dataset != dataset and row.version != version:
        raise HTTPException(
            status_code=404,
            detail=f"Could not find requested asset {dataset}/{version}/{asset_id}",
        )

    return row


@router.get(
    "/assets",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=List[Asset],
)
async def get_assets_root(
    *, asset_type: Optional[AssetType] = Query(None, title="Filter by Asset Type")
):
    """
    Get all assets
    """
    if asset_type:
        rows: List[ORMAsset] = await assets.get_assets_by_type(asset_type)
    else:
        rows = await assets.get_all_assets()

    return rows


@router.get(
    "assets/{asset_id}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=Asset,
)
async def get_asset_root(*, asset_id: UUID = Path(...)):
    """
    Get a specific asset
    """
    row: ORMAsset = await assets.get_asset(asset_id)
    return row


@router.post(
    "/{dataset}/{version}/assets",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=Asset,
    status_code=201,
)
async def add_new_asset(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    request: Optional[AssetCreateIn],
    is_authorized: bool = Depends(is_admin),
    response: Response,
):
    """
    Add a new asset to a dataset version. Managed assets will be generated by the API itself.
    In that case, the Asset URI is read only and will be set automatically.

    If the asset is not managed, you need to specify an Asset URI to link to.
    """
    # row: ORMAsset = ...
    # response.headers["Location"] = f"/{dataset}/{version}/asset/{row.asset_id}"
    # return row
    pass


@router.delete(
    "/{dataset}/{version}/assets/{asset_id}",
    response_class=ORJSONResponse,
    tags=["Assets"],
    response_model=Asset,
)
async def delete_asset(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    asset_id: UUID = Path(...),
    is_authorized: bool = Depends(is_admin),
):
    """
    Delete selected asset.
    For managed assets, all resources will be deleted. For non-managed assets, only the link will be deleted.
    """
    pass


@router.post("/{dataset}/{version}/{asset_id}/change_log", tags=["Assets"])
async def asset_history(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    asset_id: UUID = Path(...),
    request: ChangeLog,
    is_authorized: bool = Depends(is_admin),
):
    """
    Log changes for given asset
    """

    row = await assets.update_asset(asset_id, change_log=[request.dict()])

    return await _asset_response(row)


async def _asset_response(data: ORMAsset) -> Dict[str, Any]:
    """
    Serialize ORM response
    """
    response = Asset.from_orm(data).dict(by_alias=True)
    return response


async def _create_database_table():

    # supported input types
    #  - table
    #  - vector

    # steps
    #  - create table and upload data into database
    #  - inherit from geostore (vector/ polygon only)

    # creation options:
    #  - indicies (which fields get an index, what kind of index?)
    #  - partitioning (how to partition table, on what field?)
    #  - cluster (how to cluster table, based on which index?)
    #  - force field types (don't let loader guess field types, but provide list of field types instead)

    # custom metadata
    #  - fields (field name, field alias, field description, field type, is filter, is feature info)

    raise NotImplementedError


async def _create_dynamic_vector_tile_cache():

    # supported input types
    #  - vector

    # steps
    #  - wait until database table is created
    #  - create dynamic vector tile asset entry to enable service

    # creation options:
    #  - default symbology/ legend

    # custom metadata
    #  - default symbology/ legend

    raise NotImplementedError


async def _create_static_vector_tile_cache():

    # supported input types
    #  - vector

    # steps
    #  - wait until database table is created
    #  - export ndjson file
    #  - generate static vector tiles using tippecanoe and upload to S3
    #  - create static vector tile asset entry to enable service

    # creation options:
    #  - default symbology/ legend
    #  - tiling strategy
    #  - min/max zoom level
    #  - caching strategy

    # custom metadata
    #  - default symbology/ legend
    #  - rendered zoom levels

    raise NotImplementedError


async def _create_static_raster_tile_cache():

    # supported input types
    #  - raster
    #  - vector ?

    # steps
    # create raster tile cache using mapnik and upload to S3
    # register static raster tile cache asset entry to enable service

    # creation options:
    #  - symbology/ legend
    #  - tiling strategy
    #  - min/max zoom level
    #  - caching strategy

    # custom metadata
    #  - symbology/ legend
    #  - rendered zoom levels

    raise NotImplementedError


async def _create_dynamic_raster_tile_cache():
    # supported input types
    #  - raster
    #  - vector ?

    # steps
    # create raster set (pixETL) using WebMercator grid
    # register dynamic raster tile cache asset entry to enable service

    # creation options:
    #  - symbology/ legend
    #  - tiling strategy
    #  - min/max zoom level
    #  - caching strategy

    # custom metadata
    #  - symbology/ legend

    raise NotImplementedError


async def _create_tile_set():

    # supported input types
    #  - vector
    #  - raster

    # steps
    #  - wait until database table is created (vector only)
    #  - create 1x1 materialized view (vector only)
    #  - create raster tiles using pixETL and upload to S3
    #  - create tile set asset entry

    # creation options
    #  - set tile set value name
    #  - select field value or expression to use for rasterization (vector only)
    #  - select order direction (asc/desc) of field values for rasterization (vector only)
    #  - override input raster, must be another raster tile set of the same version (raster only)
    #  - define numpy calc expression (raster only)
    #  - select resampling method (raster only)
    #  - select out raster datatype
    #  - select out raster nbit value
    #  - select out raster no data value
    #  - select out raster grid type

    # custom metadata
    #  - raster statistics
    #  - raster table (pixel value look up)
    #  - list of raster files
    #  - raster data type
    #  - compression
    #  - no data value

    raise NotImplementedError
