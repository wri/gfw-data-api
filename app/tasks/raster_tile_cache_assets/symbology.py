import os
from collections import defaultdict
from typing import Any, Callable, Coroutine, DefaultDict, Dict, List, Optional, Tuple

from fastapi.logger import logger

from app.crud.assets import create_asset
from app.models.enum.assets import AssetType
from app.models.enum.creation_options import RasterDrivers
from app.models.enum.pixetl import DataType
from app.models.enum.sources import RasterSourceType
from app.models.enum.symbology import ColorMapType
from app.models.pydantic.assets import AssetCreateIn
from app.models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from app.models.pydantic.jobs import BuildRGBJob, Job
from app.models.pydantic.symbology import Symbology
from app.settings.globals import PIXETL_DEFAULT_RESAMPLING
from app.tasks import callback_constructor
from app.tasks.raster_tile_cache_assets.utils import (
    create_wm_tile_set_job,
    get_zoom_source_uri,
    reproject_to_web_mercator,
    to_tile_geojson,
)
from app.tasks.raster_tile_set_assets.utils import JOB_ENV
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
    """Skip symbology step."""
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
    """Create RGBA raster which gradient symbology based on input raster."""
    pixel_meaning = f"{source_asset_co.pixel_meaning}_gradient"
    parents = [jobs_dict[zoom_level]["source_reprojection_job"]]

    creation_options = source_asset_co.copy(
        deep=True,
        update={
            "calc": None,
            "resampling": PIXETL_DEFAULT_RESAMPLING,
            "grid": f"zoom_{zoom_level}",
            "pixel_meaning": pixel_meaning,
        },
    )
    job_name = f"{dataset}_{version}_{pixel_meaning}_{zoom_level}"
    job, uri = await create_wm_tile_set_job(
        dataset, version, creation_options, job_name, parents
    )

    return [job], uri


async def date_conf_intensity_symbology(
    dataset: str,
    version: str,
    source_asset_co: RasterTileSetSourceCreationOptions,
    zoom_level: int,
    max_zoom: int,
    jobs_dict: Dict,
) -> Tuple[List[Job], str]:
    """Create Raster Tile Set asset which combines date_conf raster and
    intensity raster into one.

    At native resolution (max_zoom) it will create intensity raster
    based on given source. For lower zoom levels if will resample higher
    zoom level tiles using bilinear resampling method. Once intensity
    raster tile set is created it will combine it with ource (date_conf)
    raster into rbg encoded raster.
    """

    pixel_meaning = "intensity"

    source_uri: Optional[List[str]] = get_zoom_source_uri(
        dataset, version, source_asset_co, zoom_level, max_zoom
    )
    intensity_source_co: RasterTileSetSourceCreationOptions = source_asset_co.copy(
        deep=True,
        update={
            "source_uri": source_uri,
            "no_data": None,
            "pixel_meaning": pixel_meaning,
        },
    )
    date_conf_job = jobs_dict[zoom_level]["source_reprojection_job"]

    if zoom_level != max_zoom:
        previous_level_intensity_reprojection_job = [
            jobs_dict[zoom_level + 1]["intensity_reprojection_job"]
        ]
    else:
        previous_level_intensity_reprojection_job = [date_conf_job]

    intensity_job, intensity_uri = await reproject_to_web_mercator(
        dataset,
        version,
        intensity_source_co,
        zoom_level,
        max_zoom,
        previous_level_intensity_reprojection_job,
        max_zoom_resampling=PIXETL_DEFAULT_RESAMPLING,
        max_zoom_calc="(A>0)*55",
    )
    jobs_dict[zoom_level]["intensity_reprojection_job"] = intensity_job

    assert source_asset_co.source_uri, "No source URI set"
    date_conf_uri = source_asset_co.source_uri[0]

    merge_job, dst_uri = await _merge_intensity_and_date_conf(
        dataset,
        version,
        to_tile_geojson(date_conf_uri),
        to_tile_geojson(intensity_uri),
        zoom_level,
        [date_conf_job, intensity_job],
    )
    jobs_dict[zoom_level]["merge_intensity_job"] = merge_job

    return [intensity_job, merge_job], dst_uri


async def _merge_intensity_and_date_conf(
    dataset: str,
    version: str,
    date_conf_uri: str,
    intensity_uri: str,
    zoom_level: int,
    parents: List[Job],
) -> Tuple[Job, str]:
    """Create RGB encoded raster tile set based on date_conf and intensity
    raster tile sets."""

    pixel_meaning = "rgb_encoded"

    encoded_co = RasterTileSetSourceCreationOptions(
        pixel_meaning=pixel_meaning,
        data_type=DataType.uint8,
        resampling=PIXETL_DEFAULT_RESAMPLING,
        overwrite=False,
        grid=f"zoom_{zoom_level}",
        symbology=Symbology(type=ColorMapType.date_conf_intensity),
        compute_stats=False,
        compute_histogram=False,
        source_type=RasterSourceType.raster,
        source_driver=RasterDrivers.geotiff,
        source_uri=[date_conf_uri, intensity_uri],
    )

    asset_uri = get_asset_uri(
        dataset,
        version,
        AssetType.raster_tile_set,
        encoded_co.dict(by_alias=True),
        "epsg:3857",
    )
    asset_prefix = asset_uri.rsplit("/", 1)[0]

    logger.debug(
        f"ATTEMPTING TO CREATE MERGED ASSET WITH THESE CREATION OPTIONS: {encoded_co}"
    )

    # Create an asset record
    asset_options = AssetCreateIn(
        asset_type=AssetType.raster_tile_set,
        asset_uri=asset_uri,
        is_managed=True,
        creation_options=encoded_co,
        metadata={},
    ).dict(by_alias=True)

    asset = await create_asset(dataset, version, **asset_options)
    logger.debug(
        f"ZOOM LEVEL {zoom_level} MERGED ASSET CREATED WITH ASSET_ID {asset.asset_id}"
    )

    command = [
        "merge_intensity.sh",
        date_conf_uri,
        intensity_uri,
        asset_prefix,
    ]

    callback = callback_constructor(asset.asset_id)

    rgb_encoding_job = BuildRGBJob(
        job_name=f"merge_intensity_zoom_{zoom_level}",
        command=command,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents],
    )

    # TODO: Run pixetl_prep afterwards to generate tiles.geojson file

    return rgb_encoding_job, os.path.join(asset_prefix, "tiles.geojson")


_symbology_constructor: Dict[str, SymbologyFuncType] = {
    ColorMapType.date_conf_intensity: date_conf_intensity_symbology,
    ColorMapType.gradient: gradient_symbology,
}

symbology_constructor: DefaultDict[str, SymbologyFuncType] = defaultdict(
    lambda: no_symbology
)
symbology_constructor.update(**_symbology_constructor)
