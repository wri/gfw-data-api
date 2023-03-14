from typing import Any, Dict, List, Optional
from uuid import UUID

import numpy as np
from fastapi import HTTPException
from fastapi.logger import logger

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
    no_symbology,
    symbology_constructor,
)
from app.tasks.raster_tile_cache_assets.utils import (
    convert_float_to_int,
    create_tile_cache,
    reproject_to_web_mercator,
)
from app.utils.path import get_asset_uri, tile_uri_to_tiles_geojson


async def raster_tile_cache_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:
    """Generate Raster Tile Cache Assets."""

    # TODO: Refactor to be easier to test

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

    # Get the creation options from the original raster tile set asset and
    # overwrite settings to make them accurate for the new asset(s) we're
    # creating. In particular the source asset had some source URI from
    # which it was imported, but we're not starting over and going from
    # THAT source URI, we're starting from the source URI of the resulting
    # source asset, which will be in the data lake. Generate that here.

    new_source_uri = [
        tile_uri_to_tiles_geojson(
            get_asset_uri(
                dataset,
                version,
                AssetType.raster_tile_set,
                source_asset.creation_options,
            )
        )
    ]

    # The first thing we do for each zoom level is reproject the source asset
    # to web-mercator. We don't want the calc string (if any) used to
    # create the source asset to be applied again to the already transformed
    # data, so set it to None.
    # Make sure source_type and source_driver are set in case it is an
    # auxiliary asset.
    wm_asset_co = RasterTileSetSourceCreationOptions(
        # TODO: With python 3.9, we can use the `|` operator here
        #  waiting for https://github.com/tiangolo/uvicorn-gunicorn-fastapi-docker/pull/67
        **{
            **source_asset.creation_options,
            **{
                "source_type": RasterSourceType.raster,
                "source_driver": RasterDrivers.geotiff,
                "source_uri": new_source_uri,
                "calc": None,
                "resampling": resampling,
                "compute_stats": False,
                "compute_histogram": False,
                "symbology": Symbology(**symbology),
                "subset": None,
            },
        }
    )

    # If float data type, convert to int in derivative assets for performance
    # FIXME: Make this work for multi-band inputs
    max_zoom_calc = None
    if wm_asset_co.data_type == DataType.boolean:
        pass  # So the next line doesn't break
    elif np.issubdtype(np.dtype(wm_asset_co.data_type), np.floating):
        logger.info("Source datatype is float subtype, converting to int")
        source_asset_co, max_zoom_calc = convert_float_to_int(
            source_asset.stats, wm_asset_co
        )

    assert wm_asset_co.symbology is not None
    symbology_function = symbology_constructor[wm_asset_co.symbology.type].function

    # We want to make sure that the final RGB asset is named after the
    # implementation of the tile cache and that the source_asset name is not
    # already used by another intermediate asset.
    # TODO: Actually make sure the intermediate assets aren't going to
    # overwrite any existing assets
    if symbology_function == no_symbology:
        wm_asset_co.pixel_meaning = implementation
    else:
        wm_asset_co.pixel_meaning = f"{wm_asset_co.pixel_meaning}_{implementation}"

    job_list: List[Job] = []
    jobs_dict: Dict[int, Dict[str, Job]] = dict()

    for zoom_level in range(max_zoom, min_zoom - 1, -1):
        jobs_dict[zoom_level] = dict()

        # If we're at the max zoom level, this is the first job that needs to
        # be run, so it has no dependencies. On subsequent zoom levels (i.e.
        # as we zoom out) we resample from the previous level, and thus must
        # depend on the previous zoom level's job.
        if zoom_level == max_zoom:
            source_reprojection_parent_jobs: List[Job] = []
        else:
            source_reprojection_parent_jobs = [
                jobs_dict[zoom_level + 1]["source_reprojection_job"]
            ]

        (
            source_reprojection_job,
            source_reprojection_uri,
        ) = await reproject_to_web_mercator(
            dataset,
            version,
            wm_asset_co,
            zoom_level,
            max_zoom,
            source_reprojection_parent_jobs,
            max_zoom_resampling=PIXETL_DEFAULT_RESAMPLING,
            max_zoom_calc=max_zoom_calc,
            use_resampler=max_zoom_calc is None,
        )
        jobs_dict[zoom_level]["source_reprojection_job"] = source_reprojection_job
        job_list.append(source_reprojection_job)

        symbology_jobs: List[Job]
        symbology_uri: str

        # Now that we've generated the job for the WM asset, we create
        # symbology-specific assets (if any). Replace the source URI of the
        # creation options once again to point to the just-created WM asset
        # on which they will be based.
        symbology_co = wm_asset_co.copy(
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

        bit_depth: int = symbology_constructor[symbology_co.symbology.type].bit_depth

        # The symbology-specific function (above) returns the URI of the
        # final asset. Create the actual tile cache based on that.
        if zoom_level <= max_static_zoom:
            tile_cache_job: Job = await create_tile_cache(
                dataset,
                version,
                symbology_uri,
                zoom_level,
                implementation,
                callback_constructor(asset_id),
                [*symbology_jobs, source_reprojection_job],
                bit_depth,
            )
            job_list.append(tile_cache_job)

    log: ChangeLog = await execute(job_list)
    return log


async def raster_tile_cache_validator(
    dataset: str, version: str, input_data: Dict[str, Any]
) -> None:
    """Validate Raster Tile Cache Creation Options.

    Used in asset route. If validation fails, it will raise an
    HTTPException visible to user.
    """
    source_asset: ORMAsset = await get_asset(
        input_data["creation_options"]["source_asset_id"]
    )
    if (source_asset.dataset != dataset) or (source_asset.version != version):
        message: str = (
            "Dataset and version of source asset must match dataset and "
            "version of current asset."
        )
        raise HTTPException(status_code=400, detail=message)

    symbology_type = input_data["creation_options"].get("symbology", {}).get("type")
    if symbology_type:
        symbology_info = symbology_constructor[symbology_type]
        req_input_bands: Optional[List[int]] = symbology_info.req_input_bands
        band_count = source_asset.creation_options.get("band_count", 1)

        if req_input_bands and (band_count not in req_input_bands):
            message = (
                f"Symbology type {symbology_type} requires a source "
                f"asset with one of {req_input_bands} bands, but has "
                f"{band_count} band(s)."
            )
            raise HTTPException(status_code=400, detail=message)
