"""Datasets can have different versions.

Versions are usually linked to different releases. Versions can be
either mutable (data can change) or immutable (data cannot change). By
default versions are immutable. Every version needs one or many source
files. These files can be a remote, publicly accessible URL or an
uploaded file. Based on the source file(s), users can create additional
assets and activate additional endpoints to view and query the dataset.
Available assets and endpoints to choose from depend on the source type.
"""

from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Union, cast

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    Query,
    Response,
    status,
)
from fastapi.logger import logger
from fastapi.responses import ORJSONResponse

from ...crud import assets
from ...crud import metadata as metadata_crud
from ...crud import versions
from ...errors import RecordAlreadyExistsError, RecordNotFoundError
from ...models.enum.assets import AssetStatus, AssetType
from ...models.orm.assets import Asset as ORMAsset
from ...models.orm.versions import Version as ORMVersion
from ...models.pydantic.asset_metadata import (
    FieldsMetadataResponse,
    RasterBandMetadata,
    RasterBandsMetadataResponse,
)
from ...models.pydantic.authentication import User
from ...models.pydantic.change_log import ChangeLog, ChangeLogResponse
from ...models.pydantic.creation_options import (
    CreationOptions,
    CreationOptionsResponse,
    TableDrivers,
    creation_option_factory,
)
from ...models.pydantic.extent import Extent, ExtentResponse
from ...models.pydantic.metadata import (
    VersionMetadata,
    VersionMetadataIn,
    VersionMetadataResponse,
    VersionMetadataUpdate,
    VersionMetadataWithParentResponse,
)
from ...models.pydantic.statistics import Stats, StatsResponse, stats_factory
from ...models.pydantic.versions import (
    Version,
    VersionAppendIn,
    VersionCreateIn,
    VersionResponse,
    VersionUpdateIn,
)
from ...routes import dataset_dependency, dataset_version_dependency, version_dependency
from ...settings.globals import TILE_CACHE_CLOUDFRONT_ID
from ...tasks.aws_tasks import flush_cloudfront_cache
from ...tasks.default_assets import append_default_asset, create_default_asset
from ...tasks.delete_assets import delete_all_assets
from . import _verify_source_file_access
from .dataset import get_owner
from .queries import _get_data_environment

router = APIRouter()


@router.get(
    "/{dataset}/{version}",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=VersionResponse,
)
async def get_version(
    *, dv: Tuple[str, str] = Depends(dataset_version_dependency)
) -> VersionResponse:
    """Get basic metadata for a given version. The list of assets only includes
    saved (non-pending and non-failed) assets and is sorted by
    the creation time of each asset."""

    dataset, version = dv
    row: ORMVersion = await versions.get_version(dataset, version)

    return await _version_response(dataset, version, row)


@router.put(
    "/{dataset}/{version}",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=VersionResponse,
    status_code=202,
)
async def add_new_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    request: VersionCreateIn,
    background_tasks: BackgroundTasks,
    user: User = Depends(get_owner),
    response: Response,
):
    """Create a version for a given dataset by uploading the tabular, vector,
    or raster asset.

    Only the dataset's owner or a user with `ADMIN` user role can do
    this operation.
    """

    input_data = request.dict(exclude_none=True, by_alias=True)
    creation_options = input_data.pop("creation_options")

    _verify_source_file_access(creation_options["source_uri"])

    # TODO: Do more to verify that any specified options are valid for
    #  the actual source file. For example, check any specified schema
    #  with ogrinfo for vector files
    #  See https://gfw.atlassian.net/browse/GTC-2235

    # Register version with DB
    try:
        new_version: ORMVersion = await versions.create_version(
            dataset, version, **input_data
        )
    except (RecordAlreadyExistsError, RecordNotFoundError) as e:
        raise HTTPException(status_code=400, detail=str(e))

    input_data["creation_options"] = creation_options

    _ = input_data.pop(
        "metadata", None
    )  # we don't include version metadata in assets anymore
    # Everything else happens in the background task asynchronously
    background_tasks.add_task(create_default_asset, dataset, version, input_data, None)

    response.headers["Location"] = f"/{dataset}/{version}"
    return await _version_response(dataset, version, new_version)


@router.patch(
    "/{dataset}/{version}",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=VersionResponse,
)
async def update_version(
    *,
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
    request: VersionUpdateIn,
    background_tasks: BackgroundTasks,
    user: User = Depends(get_owner),
):
    """Partially update a version of a given dataset.

    Update metadata or change latest tag.

    Only the dataset's owner or a user with `ADMIN` user role can do this operation.
    """
    dataset, version = dv
    input_data = request.dict(exclude_none=True, by_alias=True)

    row: ORMVersion = await versions.update_version(dataset, version, **input_data)

    # if version was tagged as `latest`
    # make sure associated `latest` routes in tile cache cloud front distribution are invalidated
    if input_data.get("is_latest"):
        tile_cache_assets: List[ORMAsset] = await assets.get_assets_by_filter(
            dataset=dataset,
            version=version,
            asset_types=[
                AssetType.dynamic_vector_tile_cache,
                AssetType.static_vector_tile_cache,
                AssetType.raster_tile_cache,
                AssetType.cog,
            ],
        )

        if tile_cache_assets:
            background_tasks.add_task(
                flush_cloudfront_cache,
                TILE_CACHE_CLOUDFRONT_ID,
                ["/_latest", f"/{dataset}/{version}/latest/*"],
            )

    return await _version_response(dataset, version, row)


@router.post(
    "/{dataset}/{version}/append",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=VersionResponse,
)
async def append_to_version(
    *,
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
    request: VersionAppendIn,
    background_tasks: BackgroundTasks,
    user: User = Depends(get_owner),
):
    """Append new data to an existing (geo)database table.

    Schema of input file must match or be a subset of previous input
    files.

    Only the dataset's owner or a user with `ADMIN` user role can do this operation.
    """
    dataset, version = dv
    _verify_source_file_access(request.dict()["source_uri"])

    default_asset: ORMAsset = await assets.get_default_asset(dataset, version)

    # TODO: Verify that original asset schema is valid for the actual source
    #  file(s) with ogrinfo
    #  See https://gfw.atlassian.net/browse/GTC-2234

    # Construct creation_options for the append request
    # For the background task, we only need the new source uri from the request
    input_data = {"creation_options": deepcopy(default_asset.creation_options)}
    input_data["creation_options"]["source_uri"] = request.source_uri

    # If source_driver is "text", this is a datapump request
    if input_data["creation_options"]["source_driver"] != TableDrivers.text:
        # Verify that source_driver matches the original source_driver
        # TODO: Ideally append source_driver should not need to match the original source_driver,
        #  but this would break other operations that expect only one source_driver
        if input_data["creation_options"]["source_driver"] != request.source_driver:
            raise HTTPException(
                status_code=400,
                detail="source_driver must match the original source_driver",
            )

        # Use layers from request if provided, else set to None if layers are in version creation_options
        if request.layers is not None:
            input_data["creation_options"]["layers"] = request.layers
        else:
            if input_data["creation_options"].get("layers") is not None:
                input_data["creation_options"]["layers"] = None

    # Use the modified input_data to append the new data
    background_tasks.add_task(
        append_default_asset, dataset, version, input_data, default_asset.asset_id
    )

    # Now update the version's creation_options to reflect the changes from the append request
    update_data = {"creation_options": deepcopy(default_asset.creation_options)}
    update_data["creation_options"]["source_uri"] += request.source_uri
    if request.layers is not None:
        if update_data["creation_options"]["layers"] is not None:
            update_data["creation_options"]["layers"] += request.layers
        else:
            update_data["creation_options"]["layers"] = request.layers
    await assets.update_asset(default_asset.asset_id, **update_data)

    version_orm: ORMVersion = await versions.get_version(dataset, version)
    return await _version_response(dataset, version, version_orm)


@router.delete(
    "/{dataset}/{version}",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=VersionResponse,
)
async def delete_version(
    *,
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
    user: User = Depends(get_owner),
    background_tasks: BackgroundTasks,
):
    """Delete a version.

    Only delete version if it is not tagged as `latest` or if it is the
    only version associated with dataset. All associated, managed assets
    will be deleted in consequence.

    Only the dataset's owner or a user with `ADMIN` user role can do this operation.
    """
    dataset, version = dv
    row: Optional[ORMVersion] = None
    rows: List[ORMVersion] = await versions.get_versions(dataset)

    for row in rows:
        if row.version == version:
            break

    if row and row.is_latest and len(rows) > 1:
        raise HTTPException(
            status_code=409,
            detail="Deletion failed."
            "You can only delete a version tagged as `latest` if no other version of the same dataset exists."
            "Change `latest` version, or delete all other versions first.",
        )

    # We check here if the version actually exists before we delete
    row = await versions.delete_version(dataset, version)

    background_tasks.add_task(delete_all_assets, dataset, version)

    return await _version_response(dataset, version, row)


@router.get(
    "/{dataset}/{version}/change_log",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=ChangeLogResponse,
)
async def get_change_log(dv: Tuple[str, str] = Depends(dataset_version_dependency)):
    dataset, version = dv
    v: ORMVersion = await versions.get_version(dataset, version)
    change_logs: List[ChangeLog] = [
        ChangeLog(**change_log) for change_log in v.change_log
    ]

    return ChangeLogResponse(data=change_logs)


@router.get(
    "/{dataset}/{version}/creation_options",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=CreationOptionsResponse,
)
async def get_creation_options(
    dv: Tuple[str, str] = Depends(dataset_version_dependency)
):
    dataset, version = dv
    asset: ORMAsset = await assets.get_default_asset(dataset, version)
    creation_options: CreationOptions = creation_option_factory(
        asset.asset_type, asset.creation_options
    )
    return CreationOptionsResponse(data=creation_options)


@router.get(
    "/{dataset}/{version}/extent",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=ExtentResponse,
)
async def get_extent(dv: Tuple[str, str] = Depends(dataset_version_dependency)):
    dataset, version = dv
    asset: ORMAsset = await assets.get_default_asset(dataset, version)
    extent: Optional[Extent] = asset.extent
    return ExtentResponse(data=extent)


@router.get(
    "/{dataset}/{version}/stats",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=StatsResponse,
)
async def get_stats(dv: Tuple[str, str] = Depends(dataset_version_dependency)):
    """Retrieve Asset Statistics."""
    dataset, version = dv
    asset: ORMAsset = await assets.get_default_asset(dataset, version)
    stats: Optional[Stats] = stats_factory(asset.asset_type, asset.stats)
    return StatsResponse(data=stats)


@router.get(
    "/{dataset}/{version}/fields",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=Union[FieldsMetadataResponse, RasterBandsMetadataResponse],
)
async def get_fields(dv: Tuple[str, str] = Depends(dataset_version_dependency)):
    """Get the fields of a version.  For a version with a vector default asset,
    these are the fields (attributes) of the features of the base vector dataset.

    For a version with a raster default asset, the fields are all the raster
    tile sets that use the same grid as the raster default asset.  Also
    included are some fields with special meaning such as 'area__ha',
    'latitude', and 'longitude'.
    """
    dataset, version = dv
    orm_asset: ORMAsset = await assets.get_default_asset(dataset, version)

    logger.debug(f"Processing default asset type {orm_asset.asset_type}")
    if orm_asset.asset_type == AssetType.raster_tile_set:
        fields = await _get_raster_fields(orm_asset)
        response = RasterBandsMetadataResponse(data=fields)
    else:
        fields = await metadata_crud.get_asset_fields_dicts(orm_asset)
        response = FieldsMetadataResponse(data=fields)

    return response


@router.get(
    "/{dataset}/{version}/metadata",
    response_class=ORJSONResponse,
    response_model=Union[VersionMetadataWithParentResponse, VersionMetadataResponse],
    tags=["Versions"],
)
async def get_metadata(
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
    include_dataset_metadata: bool = Query(
        False, description="Whether to include dataset metadata."
    ),
):
    """Get metadata record for a dataset version."""
    dataset, version = dv

    try:
        metadata = await metadata_crud.get_version_metadata(dataset, version)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if include_dataset_metadata:
        return VersionMetadataWithParentResponse(data=metadata)

    return VersionMetadataResponse(data=metadata)


@router.post(
    "/{dataset}/{version}/metadata",
    response_model=VersionMetadataResponse,
    response_class=ORJSONResponse,
    tags=["Versions"],
)
async def create_metadata(
    *,
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
    user: User = Depends(get_owner),
    request: VersionMetadataIn,
):
    """Create a metadata record for a dataset version.

    Only the dataset's owner or a user with `ADMIN` user role can do
    this operation.
    """
    dataset, version = dv
    input_data = request.dict(exclude_none=True, by_alias=True)
    try:
        metadata: VersionMetadata = await metadata_crud.create_version_metadata(
            dataset=dataset, version=version, **input_data
        )
    except RecordAlreadyExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))

    return VersionMetadataResponse(data=metadata)


@router.delete(
    "/{dataset}/{version}/metadata",
    response_model=VersionMetadataResponse,
    response_class=ORJSONResponse,
    tags=["Versions"],
)
async def delete_metadata(
    *,
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
    user: User = Depends(get_owner),
):
    """Delete metadata record for a dataset version.

    Only the dataset's owner or a user with `ADMIN` user role can do
    this operation.
    """
    dataset, version = dv

    try:
        metadata: VersionMetadata = await metadata_crud.delete_version_metadata(
            dataset, version
        )
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return VersionMetadataResponse(data=metadata)


@router.patch(
    "/{dataset}/{version}/metadata",
    response_model=VersionMetadataResponse,
    response_class=ORJSONResponse,
    tags=["Versions"],
)
async def update_metadata(
    *,
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
    user: User = Depends(get_owner),
    request: VersionMetadataUpdate,
):
    """Update metadata record for a dataset version.

    Only the dataset's owner or a user with `ADMIN` user role can do
    this operation.
    """
    dataset, version = dv
    input_data = request.dict(exclude_none=True, by_alias=True)

    metadata = await metadata_crud.update_version_metadata(
        dataset, version, **input_data
    )

    return VersionMetadataResponse(data=metadata)


async def _get_raster_fields(asset: ORMAsset) -> List[RasterBandMetadata]:
    fields: List[RasterBandMetadata] = []

    # Add in reserved fields that have special meaning.
    for reserved_field in ["area__ha", "latitude", "longitude"]:
        args: Dict[str, Any] = {"pixel_meaning": reserved_field}
        fields.append(RasterBandMetadata(**args))

    # Fetch all raster tile sets that have the same grid
    grid = asset.creation_options["grid"]
    raster_data_environment = await _get_data_environment(grid)

    logger.debug(f"Processing data environment f{raster_data_environment}")
    for layer in raster_data_environment.layers:
        field_kwargs: Dict[str, Any] = {"pixel_meaning": layer.name}
        if layer.raster_table:
            field_kwargs["values_table"] = cast(Dict[str, Any], {})
            field_kwargs["values_table"]["rows"] = [
                row for row in layer.raster_table.rows
            ]
            if layer.raster_table.default_meaning:
                field_kwargs["values_table"][
                    "default_meaning"
                ] = layer.raster_table.default_meaning

        fields.append(RasterBandMetadata(**field_kwargs))

    return fields


async def _version_response(
    dataset: str, version: str, data: ORMVersion
) -> VersionResponse:
    """Assure that version responses are parsed correctly and include
    associated assets."""

    assets: List[ORMAsset] = (
        await ORMAsset.select("asset_type", "asset_uri", "asset_id")
        .where(ORMAsset.dataset == dataset)
        .where(ORMAsset.version == version)
        .where(ORMAsset.status == AssetStatus.saved)
        .order_by(ORMAsset.created_on)
        .gino.all()
    )
    data = Version.from_orm(data).dict(by_alias=True)
    data["assets"] = [(asset[0], asset[1], str(asset[2])) for asset in assets]

    return VersionResponse(data=Version(**data))
