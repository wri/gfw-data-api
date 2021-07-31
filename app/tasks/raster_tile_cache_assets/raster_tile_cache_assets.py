import string
from typing import Any, Dict, List, Optional
from uuid import UUID

import numpy as np
from fastapi import HTTPException

from app.crud.assets import get_asset
from app.models.enum.assets import AssetType
from app.models.enum.creation_options import RasterDrivers
from app.models.enum.pixetl import DataType
from app.models.enum.sources import RasterSourceType
from app.models.orm.assets import Asset as ORMAsset
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from app.models.pydantic.jobs import Job
from app.models.pydantic.symbology import Symbology
from app.settings.globals import PIXETL_DEFAULT_RESAMPLING
from app.tasks import callback_constructor
from app.tasks.batch import execute
from app.tasks.raster_tile_cache_assets.symbology import (
    date_conf_intensity_multi_16_symbology,
    no_symbology,
    symbology_constructor,
)
from app.tasks.raster_tile_cache_assets.utils import (
    convert_float_to_int,
    create_tile_cache,
    reproject_to_web_mercator,
)
from app.utils.path import get_asset_uri


async def raster_tile_cache_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:
    """Generate Raster Tile Cache Assets."""

    min_zoom = input_data["creation_options"]["min_zoom"]
    max_zoom = input_data["creation_options"]["max_zoom"]
    max_static_zoom = input_data["creation_options"]["max_static_zoom"]
    implementation = input_data["creation_options"]["implementation"]
    symbology = input_data["creation_options"]["symbology"]
    resampling = input_data["creation_options"]["resampling"]

    # source_asset_id is currently required. Could perhaps make it optional
    # in the case that the default asset is the only one.
    source_asset: ORMAsset = await get_asset(
        input_data["creation_options"]["source_asset_id"]
    )

    # Get the creation options from the original raster tile set asset and overwrite settings
    # make sure source_type and source_driver are set in case it is an auxiliary asset

    new_source_uri = [
        get_asset_uri(
            dataset,
            version,
            AssetType.raster_tile_set,
            source_asset.creation_options,
        ).replace("{tile_id}.tif", "tiles.geojson")
    ]

    band_count = source_asset.creation_options["band_count"]
    if band_count > 1:
        # This could be more elegant, probably.
        bands_string = str(list(string.ascii_uppercase[:band_count])).replace("'", "")
        calc: Optional[str] = f"np.ma.array({bands_string})"
    else:
        calc = None

    source_asset_co = RasterTileSetSourceCreationOptions(
        # TODO: With python 3.9, we can use the `|` operator here
        #  waiting for https://github.com/tiangolo/uvicorn-gunicorn-fastapi-docker/pull/67
        **{
            **source_asset.creation_options,
            **{
                "source_type": RasterSourceType.raster,
                "source_driver": RasterDrivers.geotiff,
                "source_uri": new_source_uri,
                "calc": calc,
                "resampling": resampling,
                "compute_stats": False,
                "compute_histogram": False,
                "symbology": Symbology(**symbology),
                "subset": None,
            },
        }
    )

    # If float data type, convert to int in derivative assets for performance
    max_zoom_calc = None
    if source_asset_co.data_type == DataType.boolean:
        pass  # So the next line doesn't break
    elif np.issubdtype(np.dtype(source_asset_co.data_type), np.floating):
        source_asset_co, max_zoom_calc = convert_float_to_int(
            source_asset.stats, source_asset_co
        )

    assert source_asset_co.symbology is not None
    symbology_function = symbology_constructor[source_asset_co.symbology.type]

    # We want to make sure that the final RGB asset is named after the
    # implementation of the tile cache and that the source_asset name is not
    # already used by another intermediate asset.
    if symbology_function == no_symbology:
        source_asset_co.pixel_meaning = implementation
    else:
        source_asset_co.pixel_meaning = (
            f"{source_asset_co.pixel_meaning}_{implementation}"
        )

    job_list: List[Job] = []
    jobs_dict: Dict[int, Dict[str, Job]] = dict()

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
            max_zoom_calc=max_zoom_calc,
        )
        jobs_dict[zoom_level]["source_reprojection_job"] = source_reprojection_job
        job_list.append(source_reprojection_job)

        symbology_jobs: List[Job]
        symbology_uri: str

        symbology_co = source_asset_co.copy(
            deep=True, update={"source_uri": [source_reprojection_uri]}
        )
        symbology_jobs, symbology_uri = await symbology_function(
            dataset,
            version,
            implementation,
            symbology_co,
            zoom_level,
            max_zoom,
            jobs_dict,
        )
        job_list += symbology_jobs

        # FIXME
        if symbology_function == date_conf_intensity_multi_16_symbology:
            bit_depth: int = 16
        else:
            bit_depth = 8

        if zoom_level <= max_static_zoom:
            tile_cache_job: Job = await create_tile_cache(
                dataset,
                version,
                symbology_uri,
                zoom_level,
                implementation,
                callback_constructor(asset_id),
                symbology_jobs + [source_reprojection_job],
                bit_depth,
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

    # if source_asset.creation_options.get("band_count", 1) > 1:
    #     raise HTTPException(
    #         status_code=400, detail="Cannot apply colormap on multi-band image."
    #     )
