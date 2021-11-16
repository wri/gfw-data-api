"""Datasets can have different versions.

Versions are usually linked to different releases. Versions can be
either mutable (data can change) or immutable (data cannot change). By
default versions are immutable. Every version needs one or many source
files. These files can be a remote, publicly accessible URL or an
uploaded file. Based on the source file(s), users can create additional
assets and activate additional endpoints to view and query the dataset.
Available assets and endpoints to choose from depend on the source type.
"""
import asyncio
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response
from fastapi.logger import logger
from fastapi.responses import ORJSONResponse

from app.models.enum.versions import VersionStatus

from ...authentication.token import is_admin
from ...crud import assets, versions
from ...errors import RecordAlreadyExistsError, RecordNotFoundError
from ...models.enum.assets import AssetStatus, AssetType
from ...models.orm.assets import Asset as ORMAsset
from ...models.orm.versions import Version as ORMVersion
from ...models.pydantic.change_log import ChangeLog, ChangeLogResponse
from ...models.pydantic.creation_options import (
    CreationOptions,
    CreationOptionsResponse,
    creation_option_factory,
)
from ...models.pydantic.extent import Extent, ExtentResponse
from ...models.pydantic.metadata import (
    FieldMetadata,
    FieldMetadataResponse,
    RasterFieldMetadata,
)
from ...models.pydantic.statistics import Stats, StatsResponse, stats_factory
from ...models.pydantic.versions import (
    Version,
    VersionAppendIn,
    VersionCreateIn,
    VersionResponse,
    VersionUpdateIn,
)
from ...routes import create_dataset_version_dependency, dataset_version_dependency
from ...settings.globals import TILE_CACHE_CLOUDFRONT_ID
from ...tasks.aws_tasks import flush_cloudfront_cache
from ...tasks.default_assets import append_default_asset, create_default_asset
from ...tasks.delete_assets import delete_all_assets
from ...utils.aws import head_s3
from ...utils.path import split_s3_path
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
    dataset_version: Tuple[str, str] = Depends(create_dataset_version_dependency),
    request: VersionCreateIn,
    background_tasks: BackgroundTasks,
    is_authorized: bool = Depends(is_admin),
    response: Response,
):
    """Create or update a version for a given dataset."""
    dataset, version = dataset_version
    input_data = request.dict(exclude_none=True, by_alias=True)
    creation_options = input_data.get("creation_options")

    if "source_uri" in creation_options:
        await _verify_source_file_access(creation_options["source_uri"])

    input_data.pop("creation_options")

    # Register version with DB
    try:
        new_version: ORMVersion = await versions.create_version(
            dataset, version, **input_data
        )
    except (RecordAlreadyExistsError, RecordNotFoundError) as e:
        raise HTTPException(status_code=400, detail=str(e))

    input_data["creation_options"] = creation_options

    if creation_options.get("delete_version", None):
        await create_default_asset(dataset, version, input_data, file_obj=None)
    else:
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
    await _verify_source_file_access(request.dict()["source_uri"])

    default_asset: ORMAsset = await assets.get_default_asset(dataset, version)

    # For the background task, we only need the new source uri from the request
    input_data = {"creation_options": deepcopy(default_asset.creation_options)}
    input_data["creation_options"]["source_uri"] = request.source_uri
    background_tasks.add_task(
        append_default_asset, dataset, version, input_data, default_asset.asset_id
    )

    # We now want to append the new uris to the existing once and update the asset
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
    response_model=FieldMetadataResponse,
)
async def get_fields(dv: Tuple[str, str] = Depends(dataset_version_dependency)):
    dataset, version = dv
    asset = await assets.get_default_asset(dataset, version)

    logger.debug(f"Processing default asset type {asset.asset_type}")
    fields: Union[List[FieldMetadata], List[RasterFieldMetadata]] = []
    if asset.asset_type == AssetType.raster_tile_set:
        fields = await _get_raster_fields(asset)
    else:
        fields = [FieldMetadata(**field) for field in asset.fields]

    return FieldMetadataResponse(data=fields)


async def _get_raster_fields(asset: ORMAsset) -> List[RasterFieldMetadata]:
    fields: List[RasterFieldMetadata] = []
    grid = asset.creation_options["grid"]

    raster_data_environment = await _get_data_environment(grid)

    logger.debug(f"Processing data environment f{raster_data_environment}")
    for layer in raster_data_environment.layers:
        field_kwargs: Dict[str, Any] = {
            "field_name": layer.name,
        }

        if layer.raster_table:
            field_kwargs["field_values"] = [
                row.meaning for row in layer.raster_table.rows
            ]
            if layer.raster_table.default_meaning:
                field_kwargs["field_values"].append(layer.raster_table.default_meaning)

        fields.append(RasterFieldMetadata(**field_kwargs))

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


async def _verify_source_file_access(s3_sources: List[str]) -> None:
    head_calls = [head_s3(*split_s3_path(s3_source)) for s3_source in s3_sources]
    results = await asyncio.gather(*head_calls)
    if not all(results):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot access all of the source files {s3_sources}",
        )
