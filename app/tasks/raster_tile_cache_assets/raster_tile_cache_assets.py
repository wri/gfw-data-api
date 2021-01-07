from typing import Any, Dict, List
from uuid import UUID

from fastapi import HTTPException

from app.crud.assets import get_asset
from app.models.enum.assets import AssetType
from app.models.enum.pixetl import ResamplingMethod
from app.models.orm.assets import Asset as ORMAsset
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from app.models.pydantic.jobs import Job
from app.models.pydantic.symbology import Symbology
from app.settings.globals import PIXETL_DEFAULT_RESAMPLING
from app.tasks import callback_constructor
from app.tasks.batch import execute
from app.tasks.raster_tile_cache_assets.symbology import symbology_constructor
from app.tasks.raster_tile_cache_assets.utils import (
    create_tile_cache,
    reproject_to_web_mercator,
)
from app.utils.path import get_asset_uri


async def raster_tile_cache_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:
    """Generate Raster Tile Cache Assets."""

    min_zoom = input_data["creation_options"]["min_zoom"]
    max_zoom = input_data["creation_options"]["max_zoom"]
    max_static_zoom = input_data["creation_options"]["max_static_zoom"]
    implementation = input_data["creation_options"]["implementation"]
    symbology = input_data["creation_options"]["symbology"]

    # source_asset_id is currently required. Could perhaps make it optional
    # in the case that the default asset is the only one.
    source_asset: ORMAsset = await get_asset(
        input_data["creation_options"]["source_asset_id"]
    )
    # Get the creation options from the original raster tile set asset
    source_asset_co = RasterTileSetSourceCreationOptions(
        **source_asset.creation_options
    )

    source_asset_co.calc = None
    source_asset_co.source_uri = [
        get_asset_uri(
            dataset,
            version,
            AssetType.raster_tile_set,
            source_asset_co.dict(by_alias=True),
        ).replace("{tile_id}.tif", "tiles.geojson")
    ]
    # TODO:
    #  Using med over mode, due to performance issues.
    #  Need to verify if this is the best resampling method for data other than RADD alerts as well.
    source_asset_co.resampling = ResamplingMethod.med
    source_asset_co.symbology = Symbology(**symbology)
    source_asset_co.compute_stats = False
    source_asset_co.compute_histogram = False

    job_list: List[Job] = []
    jobs_dict: Dict[int, Dict[str, Job]] = dict()

    symbology_function = symbology_constructor[symbology["type"]]

    for zoom_level in range(max_zoom, min_zoom - 1, -1):
        jobs_dict[zoom_level] = dict()
        source_projection_parent_job = jobs_dict.get(zoom_level + 1, {}).get(
            "source_reprojection_job"
        )

        source_projection_parent_jobs = (
            [source_projection_parent_job] if source_projection_parent_job else []
        )

        (
            source_reprojection_job,
            source_reprojection_uri,
        ) = await reproject_to_web_mercator(
            dataset,
            version,
            source_asset_co,
            zoom_level,
            max_zoom,
            source_projection_parent_jobs,
            max_zoom_resampling=PIXETL_DEFAULT_RESAMPLING,
        )
        jobs_dict[zoom_level]["source_reprojection_job"] = source_reprojection_job
        job_list.append(source_reprojection_job)

        symbology_jobs: List[Job]
        symbology_uri: str

        symbology_co = source_asset_co.copy(
            deep=True, update={"source_uri": [source_reprojection_uri]}
        )
        symbology_jobs, symbology_uri = await symbology_function(
            dataset, version, symbology_co, zoom_level, max_zoom, jobs_dict,
        )
        job_list += symbology_jobs

        if zoom_level <= max_static_zoom:
            tile_cache_job: Job = await create_tile_cache(
                dataset,
                version,
                symbology_uri,
                zoom_level,
                implementation,
                callback_constructor(asset_id),
                symbology_jobs + [source_reprojection_job],
            )
            job_list.append(tile_cache_job)

    log: ChangeLog = await execute(job_list)
    return log


async def raster_tile_cache_validator(
    dataset: str, version: str, input_data: Dict[str, Any]
) -> None:
    """Validate Raster Tile Cache Creation Options.

    Used in asset route. If validation fails, it will raise a
    HTTPException visible to user.
    """
    source_asset: ORMAsset = await get_asset(
        input_data["creation_options"]["source_asset_id"]
    )
    if (source_asset.dataset != dataset) or (source_asset.version != version):
        raise HTTPException(
            status_code=400,
            detail="Dataset and version of source asset must match dataset and version of current asset.",
        )
