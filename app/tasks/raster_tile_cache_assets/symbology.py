import os
from collections import defaultdict
from typing import Any, Callable, Coroutine, DefaultDict, Dict, List, Optional, Tuple

from fastapi.logger import logger

from app.crud.assets import create_asset
from app.models.enum.assets import AssetType
from app.models.enum.symbology import ColorMapType
from app.models.pydantic.assets import AssetCreateIn
from app.models.pydantic.creation_options import (
    RasterTileSetAssetCreationOptions,
    RasterTileSetSourceCreationOptions,
)
from app.models.pydantic.jobs import BuildRGBJob, Job
from app.settings.globals import PIXETL_DEFAULT_RESAMPLING
from app.tasks import callback_constructor
from app.tasks.raster_tile_cache_assets.utils import JOB_ENV, reproject_to_web_mercator
from app.utils.path import get_asset_uri

SymbologyFuncType = Callable[
    [str, str, RasterTileSetSourceCreationOptions, int, int, Dict[Any, Any]],
    Coroutine[Any, Any, Tuple[List[Job], str]],
]


async def no_symbology(
    dataset: str,
    version: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    if source_asset_co.source_uri:
        return list(), source_asset_co.source_uri[0]
    else:
        raise RuntimeError("No source URI set.")


async def gradient_symbology(
    dataset: str,
    version: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    raise NotImplementedError("Gradient symbology not implemented")


async def date_conf_intensity_symbology(
    dataset: str,
    version: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    pixel_meaning = "intensity"

    source_uri = (
        source_asset_co.source_uri
        if not zoom_level == max_zoom
        else [
            get_asset_uri(
                dataset,
                version,
                AssetType.raster_tile_set,
                source_asset_co.dict(by_alias=True),
            ).replace("{tile_id}.tif", "tiles.geojson")
        ]
    )
    intensity_source_co = source_asset_co.copy(
        deep=True,
        update={
            "source_uri": source_uri,
            "no_data": None,
            "pixel_meaning": pixel_meaning,
        },
    )

    previous_level_intensity_reprojection_job: Optional[Job] = None
    if zoom_level != max_zoom:
        previous_level_intensity_reprojection_job = jobs_dict[zoom_level + 1][
            "intensity_reprojection_job"
        ]

    intensity_reprojection_job: Job = await reproject_to_web_mercator(
        dataset,
        version,
        intensity_source_co,
        zoom_level,
        max_zoom,
        previous_level_intensity_reprojection_job,
        max_zoom_resampling=PIXETL_DEFAULT_RESAMPLING,
        max_zoom_calc="(A>0)*55",
    )
    jobs_dict[zoom_level]["intensity_reprojection_job"] = intensity_reprojection_job

    source_reprojection_job = jobs_dict[zoom_level]["source_reprojection_job"]

    intensity_co = source_asset_co.copy(
        deep=True,
        update={"pixel_meaning": pixel_meaning, "grid": f"zoom_{zoom_level}"},
    )

    date_conf_co = source_asset_co.copy(
        deep=True, update={"grid": f"zoom_{zoom_level}"}
    )

    merge_job, dst_uri = await _merge_intensity_and_date_conf(
        dataset,
        version,
        date_conf_co,
        intensity_co,
        zoom_level,
        [source_reprojection_job, intensity_reprojection_job],
    )
    jobs_dict[zoom_level]["merge_intensity_job"] = merge_job

    return [intensity_reprojection_job, merge_job], dst_uri


async def _merge_intensity_and_date_conf(
    dataset: str,
    version: str,
    date_conf_co: RasterTileSetSourceCreationOptions,
    intensity_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    parents: List[Job],
) -> Tuple[Job, str]:
    pixel_meaning = "rgb_encoded"

    date_conf_uri = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        date_conf_co.dict(by_alias=True),
        "epsg:3857",
    ).replace("{tile_id}.tif", "tiles.geojson")

    intensity_uri = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        intensity_co.dict(by_alias=True),
        "epsg:3857",
    ).replace("{tile_id}.tif", "tiles.geojson")

    encoded_co_dict = intensity_co.dict(by_alias=True)
    encoded_co_dict["pixel_meaning"] = pixel_meaning

    merged_asset_uri = get_asset_uri(
        dataset, version, AssetType.raster_tile_set, encoded_co_dict, "epsg:3857"
    )
    merged_asset_prefix = merged_asset_uri.rsplit("/", 1)[0]

    del encoded_co_dict["source_uri"]
    del encoded_co_dict["source_driver"]
    del encoded_co_dict["source_type"]

    encoded_co = RasterTileSetAssetCreationOptions(**encoded_co_dict)

    logger.debug(
        f"ATTEMPTING TO CREATE MERGED ASSET WITH THESE CREATION OPTIONS: {encoded_co}"
    )

    # Create an asset record
    asset_options = AssetCreateIn(
        asset_type=AssetType.raster_tile_set,
        asset_uri=merged_asset_uri,
        is_managed=True,
        creation_options=encoded_co,
        metadata={},
    ).dict(by_alias=True)

    wm_asset_record = await create_asset(dataset, version, **asset_options)
    logger.debug(
        f"ZOOM LEVEL {zoom_level} MERGED ASSET CREATED WITH ASSET_ID {wm_asset_record.asset_id}"
    )

    command = [
        "merge_intensity.sh",
        date_conf_uri,
        intensity_uri,
        merged_asset_prefix,
    ]

    callback = callback_constructor(wm_asset_record.asset_id)

    merge_intensity_job = BuildRGBJob(
        job_name=f"merge_intensity_zoom_{zoom_level}",
        command=command,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents],
    )

    return merge_intensity_job, os.path.join(merged_asset_prefix, "tiles.geojson")


_symbology_constructor: Dict[str, SymbologyFuncType] = {
    ColorMapType.date_conf_intensity: date_conf_intensity_symbology,
    ColorMapType.gradient: gradient_symbology,
}

symbology_constructor: DefaultDict[str, SymbologyFuncType] = defaultdict(
    lambda: no_symbology
)
symbology_constructor.update(**_symbology_constructor)
