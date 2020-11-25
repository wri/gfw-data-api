import copy
import json
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi.encoders import jsonable_encoder

from app.crud.assets import create_asset, get_asset
from app.models.enum.assets import AssetType
from app.models.enum.pixetl import ResamplingMethod
from app.models.orm.assets import Asset as ORMAsset
from app.models.pydantic.assets import AssetCreateIn
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import (
    RasterTileSetAssetCreationOptions,
    RasterTileSetSourceCreationOptions,
)
from app.models.pydantic.jobs import BuildRGBJob, GDAL2TilesJob, Job, PixETLJob
from app.settings.globals import (
    AWS_REGION,
    ENV,
    PIXETL_CORES,
    PIXETL_MAX_MEM,
    S3_ENTRYPOINT_URL,
    TILE_CACHE_BUCKET,
)
from app.tasks import Callback, callback_constructor, writer_secrets
from app.tasks.batch import execute
from app.utils.path import get_asset_uri

INTENSITY_PIXEL_MEANING = "intensity"
RGB_ENCODED_PIXEL_MEANING = "rgb_encoded"

JOB_ENV = writer_secrets + [
    {"name": "AWS_REGION", "value": AWS_REGION},
    {"name": "ENV", "value": ENV},
    {"name": "CORES", "value": PIXETL_CORES},
    {"name": "MAX_MEM", "value": PIXETL_MAX_MEM},
]

if S3_ENTRYPOINT_URL:
    # Why both? Because different programs (boto,
    # pixetl, gdal*) use different vars.
    JOB_ENV = JOB_ENV + [
        {"name": "AWS_ENDPOINT_URL", "value": S3_ENTRYPOINT_URL},
        {"name": "ENDPOINT_URL", "value": S3_ENTRYPOINT_URL},
    ]


async def raster_tile_cache_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:
    # Argument validation
    min_zoom = input_data["creation_options"]["min_zoom"]
    max_zoom = input_data["creation_options"]["max_zoom"]
    max_static_zoom = input_data["creation_options"]["max_static_zoom"]
    implementation = input_data["creation_options"]["implementation"]

    assert min_zoom <= max_zoom  # FIXME: Raise appropriate exception
    assert max_static_zoom <= max_zoom  # FIXME: Raise appropriate exception

    # FIXME: Remove this when implementing standard tile cache code path:
    if (
        input_data["creation_options"]["symbology"]["color_map"]["type"]
        != "date_conf_intensity"
    ):
        raise NotImplementedError(
            "Raster tile cache currently only implemented for GLAD/RADD pipeline"
        )

    job_list: List[Job] = []

    # source_asset_id is currently required. Could perhaps make it optional
    # in the case that the default asset is the only one.
    source_asset: ORMAsset = await get_asset(
        input_data["creation_options"]["source_asset_id"]
    )

    # We should require that the source asset be of the same dataset
    # and version as the tile cache, right?
    assert source_asset.dataset == dataset  # FIXME: Raise appropriate exception
    assert source_asset.version == version  # FIXME: Raise appropriate exception

    # Re-project the original asset to web-mercator (as new assets)
    # Get the creation options from the original raster tile set asset
    source_asset_co = RasterTileSetSourceCreationOptions(
        **source_asset.creation_options
    ).dict(by_alias=True)

    # Reset calc to prevent it from interfering with new derivative assets
    # and set source_uri from whatever it is to refer to the raster tile set
    # copy in the data lake
    source_asset_co["calc"] = None
    source_asset_co["resampling"] = ResamplingMethod.med
    source_source_uri = get_asset_uri(
        dataset, version, AssetType.raster_tile_set, source_asset_co
    ).replace("{tile_id}.tif", "tiles.geojson")
    source_asset_co["source_uri"] = [source_source_uri]

    # print(f"SOURCE ASSET CREATION OPTIONS: {json.dumps(date_conf_co, indent=2)}")

    source_reprojection_jobs = await _reproject_to_web_mercator(
        dataset,
        version,
        copy.deepcopy(source_asset_co),
        min_zoom,
        max_static_zoom,
        None,
    )
    job_list += source_reprojection_jobs
    print("SOURCE ASSET REPROJECTION JOBS CREATED")

    # For GLAD/RADD, create intensity asset with pixetl and merge with
    # existing date/conf layer to form a new RGB_ENCODED_PIXEL_MEANING asset
    if (
        input_data["creation_options"]["symbology"]["color_map"]["type"]
        == "date_conf_intensity"
    ):

        # Create intensity asset from date_conf asset creation options
        # No need for a WGS84 copy, so go right to web-mercator
        date_conf_co = copy.deepcopy(source_asset_co)
        intensity_source_uri = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, date_conf_co
        ).replace("{tile_id}.tif", "tiles.geojson")

        intensity_co_dict = {
            **date_conf_co,
            **{
                "source_uri": [intensity_source_uri],
                "pixel_meaning": INTENSITY_PIXEL_MEANING,
                "resampling": ResamplingMethod.med,
                "calc": "(A>0)*55",
                "grid": date_conf_co["grid"],
                "overwrite": True,
                "no_data": None,
            },
        }

        intensity_co = RasterTileSetSourceCreationOptions(**intensity_co_dict)
        print(
            "INTENSITY ASSET CREATION OPTIONS: "
            f"{json.dumps(intensity_co.dict(by_alias=True), indent=2)}"
        )

        intensity_reprojection_jobs = await _reproject_to_web_mercator(
            dataset,
            version,
            intensity_co.dict(by_alias=True),
            min_zoom,
            max_static_zoom,
            None,
        )
        job_list += intensity_reprojection_jobs
        print("INTENSITY REPROJECTION JOBS CREATED")

        # Now merge the date/conf and intensity tiles for each zoom level to
        # create the final raster tile set asset

        # Create merged asset record
        merge_jobs = await _merge_intensity_and_date_conf(
            dataset,
            version,
            copy.deepcopy(date_conf_co),
            intensity_co.dict(by_alias=True),
            min_zoom,
            max_static_zoom,
            job_list,
        )
        job_list += merge_jobs

        # build_rgb created the merged rasters but not tiles.geojson or extent.geojson
        # FIXME: Create those now with pixetl's pixetl_prep

        # Actually create the tile cache using gdal2tiles
        print("Now create the tile cache using gdal2tiles...")
        tile_cache_co = intensity_co.dict(by_alias=True)
        tile_cache_co["pixel_meaning"] = RGB_ENCODED_PIXEL_MEANING
        tile_cache_co["srid"] = "epsg-3857"

        tile_cache_jobs = await _create_tile_cache(
            dataset,
            version,
            tile_cache_co,
            min_zoom,
            max_static_zoom,
            implementation,
            callback_constructor(asset_id),
            merge_jobs,
        )
        job_list += tile_cache_jobs

    log: ChangeLog = await execute(job_list)

    return log


async def _run_pixetl(
    dataset: str,
    version: str,
    co: Dict[str, Any],
    job_name: str,
    callback: Callback,
    parents: Optional[List[Job]] = None,
):
    co_copy = copy.deepcopy(co)
    co_copy["source_uri"] = co_copy.pop("source_uri")[0]
    overwrite = co_copy.pop("overwrite", False)
    subset = co_copy.pop("subset", None)

    command = [
        "run_pixetl.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-j",
        json.dumps(jsonable_encoder(co_copy)),
    ]

    if overwrite:
        command += ["--overwrite"]

    if subset:
        command += ["--subset", subset]

    pixetl_job_id = PixETLJob(
        job_name=job_name,
        command=command,
        environment=JOB_ENV,
        callback=callback,
        parents=[parent.job_name for parent in parents] if parents else None,
    )

    return pixetl_job_id


async def _reproject_to_web_mercator(
    dataset: str,
    version: str,
    source_creation_options: Dict[str, Any],
    min_zoom: int,
    max_zoom: int,
    parents: Optional[List[Job]],
) -> List[Job]:
    reprojection_jobs: List[Job] = []

    # Processing chokes on large datasets if we go directly to low
    # zoom levels, so start at the highest and work our way back

    co = copy.deepcopy(source_creation_options)
    co["resampling"] = "med"
    co["overwrite"] = co.get("overwrite", False)
    co["srid"] = "epsg-3857"

    for zoom_level in reversed(range(min_zoom, max_zoom)):
        co["grid"] = f"zoom_{zoom_level}"

        asset_uri = get_asset_uri(dataset, version, AssetType.raster_tile_set, co)
        del co["srid"]

        co_obj = RasterTileSetSourceCreationOptions(**co)

        # Create an asset record
        asset_options = AssetCreateIn(
            asset_type=AssetType.raster_tile_set,
            asset_uri=asset_uri,
            is_managed=True,
            creation_options=co_obj,
            metadata={},
        ).dict(by_alias=True)
        wm_asset_record = await create_asset(dataset, version, **asset_options)
        print(f"ZOOM LEVEL {zoom_level} REPROJECTION ASSET CREATED")

        zoom_level_job = await _run_pixetl(
            dataset,
            version,
            wm_asset_record.creation_options,
            f"zoom_level_{zoom_level}_{co['pixel_meaning']}_reprojection",
            callback_constructor(wm_asset_record.asset_id),
            parents=(parents + reprojection_jobs) if parents else reprojection_jobs,
        )
        reprojection_jobs.append(zoom_level_job)
        print(f"ZOOM LEVEL {zoom_level} REPROJECTION JOB CREATED")

        co["srid"] = "epsg-3857"
        source_uri = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, co
        ).replace("{tile_id}.tif", "tiles.geojson")
        co["source_uri"] = [source_uri]

    return reprojection_jobs


async def _merge_intensity_and_date_conf(
    dataset: str,
    version: str,
    date_conf_co: Dict[str, Any],
    intensity_co: Dict[str, Any],
    min_zoom: int,
    max_zoom: int,
    parents: List[Job],
):
    merge_intensity_jobs: List[Job] = []

    for zoom_level in range(min_zoom, max_zoom):
        # Sanitize creation_options

        d_c_co = copy.deepcopy(date_conf_co)
        d_c_co["srid"] = "epsg-3857"
        d_c_co["grid"] = f"zoom_{zoom_level}"
        date_conf_uri = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, d_c_co
        ).replace("{tile_id}.tif", "tiles.geojson")

        i_co = copy.deepcopy(intensity_co)
        i_co["srid"] = "epsg-3857"
        i_co["grid"] = f"zoom_{zoom_level}"
        intensity_uri = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, i_co
        ).replace("{tile_id}.tif", "tiles.geojson")

        c_co = copy.deepcopy(intensity_co)
        c_co["srid"] = "epsg-3857"
        c_co["grid"] = f"zoom_{zoom_level}"
        c_co["pixel_meaning"] = RGB_ENCODED_PIXEL_MEANING
        asset_uri = get_asset_uri(dataset, version, AssetType.raster_tile_set, c_co)
        merged_asset_prefix = asset_uri.rsplit("/", 1)[0]

        del c_co["source_uri"]
        del c_co["source_driver"]
        del c_co["source_type"]
        del c_co["srid"]

        co_obj = RasterTileSetAssetCreationOptions(**c_co)

        print(f"ATTEMPTING TO CREATE MERGED ASSET WITH THESE CREATION OPTIONS: {c_co}")

        # Create an asset record
        asset_options = AssetCreateIn(
            asset_type=AssetType.raster_tile_set,
            asset_uri=asset_uri,
            is_managed=True,
            creation_options=co_obj,
            metadata={},
        ).dict(by_alias=True)

        wm_asset_record = await create_asset(dataset, version, **asset_options)
        print(
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
        merge_intensity_jobs += [merge_intensity_job]

    return merge_intensity_jobs


async def _create_tile_cache(
    dataset: str,
    version: str,
    r_t_s_creation_options: Dict[str, Any],
    min_zoom: int,
    max_zoom: int,
    implementation: str,
    callback: Callback,
    parents: List[Job],
):
    tile_cache_jobs: List[Job] = []

    for zoom_level in range(min_zoom, max_zoom):
        # Sanitize creation_options

        co = copy.deepcopy(r_t_s_creation_options)
        co["grid"] = f"zoom_{zoom_level}"
        asset_prefix = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, co
        ).rsplit("/", 1)[0]

        print(
            f"CREATING TILE CACHE JOB FOR ZOOM LEVEL {zoom_level} WITH PREFIX {asset_prefix}"
        )

        command: List[str] = [
            "raster_tile_cache.sh",
            "-d",
            dataset,
            "-v",
            version,
            "-I",
            implementation,
            "--target_bucket",
            TILE_CACHE_BUCKET,
            "--zoom_level",
            str(zoom_level),
            asset_prefix,
        ]

        tile_cache_job = GDAL2TilesJob(
            job_name=f"generate_tile_cache_zoom_{zoom_level}",
            command=command,
            environment=JOB_ENV,
            callback=callback,
            parents=[parent.job_name for parent in parents],
        )
        tile_cache_jobs += [tile_cache_job]

    return tile_cache_jobs
