"""Datasets can have different versions.

Versions are usually linked to different releases. Versions can be
either mutable (data can change) or immutable (data cannot change). By
default versions are immutable. Every version needs one or many source
files. These files can be a remote, publicly accessible URL or an
uploaded file. Based on the source file(s), users can create additional
assets and activate additional endpoints to view and query the dataset.
Available assets and endpoints to choose from depend on the source type.
"""
from collections import defaultdict
from copy import deepcopy
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse

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

from ...authentication.token import is_admin
from ...crud import assets
from ...crud import metadata as metadata_crud
from ...crud import versions
from ...errors import RecordAlreadyExistsError, RecordNotFoundError
from ...models.enum.assets import AssetStatus, AssetType
from ...models.orm.assets import Asset as ORMAsset
from ...models.orm.versions import Version as ORMVersion
from ...models.pydantic.asset_metadata import (
    FieldMetadata,
    FieldMetadataOut,
    FieldsMetadataResponse,
    RasterBandMetadata,
    RasterBandsMetadataResponse,
)
from ...models.pydantic.change_log import ChangeLog, ChangeLogResponse
from ...models.pydantic.creation_options import (
    CreationOptions,
    CreationOptionsResponse,
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
from ...utils.aws import get_aws_files
from ...utils.google import get_gs_files
from .queries import _get_data_environment
from typing import cast

router = APIRouter()

SUPPORTED_FILE_EXTENSIONS: Sequence[str] = (
    ".csv",
    ".geojson",
    ".gpkg",
    ".ndjson",
    ".shp",
    ".tif",
    ".tsv",
    ".zip",
)

# I cannot seem to satisfy mypy WRT the type of this default dict. Last thing I tried:
# DefaultDict[str, Callable[[str, str, int, int, ...], List[str]]]
source_uri_lister_constructor = defaultdict((lambda: lambda w, x, limit=None, exit_after_max=None, extensions=None: list()))  # type: ignore
source_uri_lister_constructor.update(**{"gs": get_gs_files, "s3": get_aws_files})  # type: ignore


@router.get(
    "/{dataset}/{version}",
    response_class=ORJSONResponse,
    tags=["Versions"],
    response_model=VersionResponse,
)
async def get_version(
    *, dv: Tuple[str, str] = Depends(dataset_version_dependency)
) -> VersionResponse:
    """Get basic metadata for a given version."""

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
    is_authorized: bool = Depends(is_admin),
    response: Response,
):
    """Create or update a version for a given dataset."""

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
    is_authorized: bool = Depends(is_admin),
):
    """Partially update a version of a given dataset.

    Update metadata or change latest tag
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
    deprecated=True,
)
async def append_to_version(
    *,
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
    request: VersionAppendIn,
    background_tasks: BackgroundTasks,
    is_authorized: bool = Depends(is_admin),
):
    """Append new data to an existing (geo)database table.

    Schema of input file must match or be a subset of previous input
    files.
    """
    dataset, version = dv
    _verify_source_file_access(request.dict()["source_uri"])

    default_asset: ORMAsset = await assets.get_default_asset(dataset, version)

    # TODO: Verify that original asset schema is valid for the actual source
    #  file(s) with ogrinfo
    #  See https://gfw.atlassian.net/browse/GTC-2234

    # For the background task, we only need the new source uri from the request
    input_data = {"creation_options": deepcopy(default_asset.creation_options)}
    input_data["creation_options"]["source_uri"] = request.source_uri
    background_tasks.add_task(
        append_default_asset, dataset, version, input_data, default_asset.asset_id
    )

    # We now want to append the new uris to the existing ones and update the asset
    update_data = {"creation_options": deepcopy(default_asset.creation_options)}
    update_data["creation_options"]["source_uri"] += request.source_uri
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
    is_authorized: bool = Depends(is_admin),
    background_tasks: BackgroundTasks,
):

    """Delete a version.

    Only delete version if it is not tagged as `latest` or if it is the
    only version associated with dataset. All associated, managed assets
    will be deleted in consequence.
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
    is_authorized: bool = Depends(is_admin),
    request: VersionMetadataIn,
):
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
    is_authorized: bool = Depends(is_admin),
):
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
    is_authorized: bool = Depends(is_admin),
    request: VersionMetadataUpdate,
):
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
        field_kwargs: Dict[str, Any] = {
            "pixel_meaning": layer.name
        }
        if layer.raster_table:
            field_kwargs["values_table"] = cast(Dict[str, Any], {})
            field_kwargs["values_table"]["rows"] = [
                 row for row in layer.raster_table.rows
            ]
            if layer.raster_table.default_meaning:
                field_kwargs["values_table"]["default_meaning"] = layer.raster_table.default_meaning

        fields.append(RasterBandMetadata(**field_kwargs))

    return fields


async def _version_response(
    dataset: str, version: str, data: ORMVersion
) -> VersionResponse:
    """Assure that version responses are parsed correctly and include
    associated assets."""

    assets: List[ORMAsset] = (
        await ORMAsset.select("asset_type", "asset_uri")
        .where(ORMAsset.dataset == dataset)
        .where(ORMAsset.version == version)
        .where(ORMAsset.status == AssetStatus.saved)
        .gino.all()
    )
    data = Version.from_orm(data).dict(by_alias=True)
    data["assets"] = [(asset[0], asset[1]) for asset in assets]

    return VersionResponse(data=Version(**data))


def _verify_source_file_access(sources: List[str]) -> None:

    # TODO:
    # 1. Making the list functions asynchronous and using asyncio.gather
    # to check for valid sources in a non-blocking fashion would be good.
    # Perhaps use the aioboto3 package for aws, gcloud-aio-storage for gcs.
    # 2. It would be nice if the acceptable file extensions were passed
    # into this function so we could say, for example, that there must be
    # TIFFs found for a new raster tile set, but a CSV is required for a new
    # vector tile set version. Even better would be to specify whether
    # paths to individual files or "folders" (prefixes) are allowed.

    invalid_sources: List[str] = list()

    for source in sources:
        url_parts = urlparse(source, allow_fragments=False)
        list_func = source_uri_lister_constructor[url_parts.scheme.lower()]
        bucket = url_parts.netloc
        prefix = url_parts.path.lstrip("/")

        # Allow pseudo-globbing: Tolerate a "*" at the end of a
        # src_uri entry to allow partial prefixes (for example
        # /bucket/prefix_part_1/prefix_fragment* will match
        # /bucket/prefix_part_1/prefix_fragment_1.tif and
        # /bucket/prefix_part_1/prefix_fragment_2.tif, etc.)
        # If the prefix doesn't end in "*" or an acceptable file extension
        # add a "/" to the end of the prefix to enforce it being a "folder".
        new_prefix: str = prefix
        if new_prefix.endswith("*"):
            new_prefix = new_prefix[:-1]
        elif not new_prefix.endswith("/") and not any(
            [new_prefix.endswith(suffix) for suffix in SUPPORTED_FILE_EXTENSIONS]
        ):
            new_prefix += "/"

        if not list_func(
            bucket,
            new_prefix,
            limit=10,
            exit_after_max=1,
            extensions=SUPPORTED_FILE_EXTENSIONS,
        ):
            invalid_sources.append(source)

    if invalid_sources:
        raise HTTPException(
            status_code=400,
            detail=(
                "Cannot access all of the source files. "
                f"Invalid sources: {invalid_sources}"
            ),
        )
