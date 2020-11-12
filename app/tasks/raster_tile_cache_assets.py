import copy
import json
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi.encoders import jsonable_encoder

from app.crud.assets import create_asset, get_default_asset
from app.models.enum.assets import AssetType
from app.models.pydantic.assets import AssetCreateIn
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import (
    RasterTileSetAssetCreationOptions,
    RasterTileSetSourceCreationOptions,
)
from app.models.pydantic.jobs import BuildRGBJob, PixETLJob
from app.settings.globals import ENV, PIXETL_CORES, PIXETL_MAX_MEM, S3_ENTRYPOINT_URL
from app.tasks import Callback, callback_constructor, writer_secrets
from app.tasks.batch import execute
from app.utils.path import get_asset_uri

job_env = writer_secrets + [
    {"name": "ENV", "value": ENV},
    {"name": "CORES", "value": PIXETL_CORES},
    {"name": "MAX_MEM", "value": PIXETL_MAX_MEM},
]

if S3_ENTRYPOINT_URL:
    job_env = job_env + [
        {"name": "AWS_S3_ENDPOINT", "value": S3_ENTRYPOINT_URL},
        {"name": "AWS_ENDPOINT_URL", "value": S3_ENTRYPOINT_URL},
    ]


async def _run_pixetl(
    dataset: str,
    version: str,
    co: Dict[str, Any],
    job_name: str,
    callback: Callback,
    parents: Optional[List[str]] = None,
):
    # FIXME: Create an Asset in the DB to track intensity asset in S3
    # FIXME: See if "intensity" asset already exists first

    co["source_uri"] = co.pop("source_uri")[0]
    # co["source_uri"] = get_asset_uri(dataset, version, AssetType.raster_tile_set, co).replace("{tile_id}.tif", "tiles.geojson")
    overwrite = co.pop("overwrite", False)  # FIXME: Think about this value some more
    subset = co.pop("subset", None)

    command = [
        "run_pixetl.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-j",
        json.dumps(jsonable_encoder(co)),
    ]

    if overwrite:
        command += ["--overwrite"]

    if subset:
        command += ["--subset", subset]

    pixetl_job_id = PixETLJob(
        job_name=job_name,
        command=command,
        environment=job_env,
        callback=callback,
        parents=parents,
    )

    return pixetl_job_id


# async def _reproject_to_web_mercator(
#     dataset: str,
#     version: str,
#     ormasset: Asset,
#     source_uri: str,
#     job_env: List[Dict[str, str]],
#     callback: Callback,
# ):
#     co = ormasset.creation_options
#
#     # FIXME: Create an Asset in the DB to track asset in S3
#
#     layer_def = {
#         "source_uri": source_uri,
#         "source_type": "raster",
#         "data_type": co["data_type"],
#         "pixel_meaning": "intensity",
#         "grid": co["grid"],
#         "resampling": "mode",
#         "nbits": co["nbits"],
#         "no_data": co["no_data"]
#     }
#
#     overwrite = True  # FIXME: Think about this value some more
#     subset = co["subset"]
#
#     command = [
#         "run_pixetl.sh",
#         "-d",
#         dataset,
#         "-v",
#         version,
#         "-j",
#         json.dumps(jsonable_encoder(layer_def)),
#     ]
#
#     if overwrite:
#         command += ["--overwrite"]
#
#     if subset:
#         command += ["--subset", subset]
#
#     create_intensity_job = PixETLJob(
#         job_name="reproject_tile_set",
#         command=command,
#         environment=job_env,
#         callback=callback,
#     )
#
#     return create_intensity_job

# async def _merge_intensity_and_date_conf(
#     dataset: str,
#     version: str,
#     date_conf_uri: str,
#     intensity_uri: str,
#     job_env: List[Dict[str, str]],
#     callback: Callback,
# ):
#
#     command = [
#         "merge_intensity.sh",
#         "-d",
#         dataset,
#         "-v",
#         version,
#         date_conf_uri,
#         intensity_uri
#     ]
#
#     merge_intensity_job = BuildRGBJob(
#         job_name="merge_intensity_and_date_conf_assets",
#         command=command,
#         environment=job_env,
#         callback=callback,
#     )
#
#     return merge_intensity_job


async def raster_tile_cache_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:
    # Argument validation
    min_zoom = input_data["creation_options"]["min_zoom"]
    max_zoom = input_data["creation_options"]["max_zoom"]
    # FIXME: Make sure max_static < max_zoom, or something
    # max_static_zoom = input_data["creation_options"]["max_static_zoom"]
    assert min_zoom <= max_zoom  # FIXME: Raise appropriate exception

    # What is needed to create a raster tile cache?
    # Should default asset be a raster tile set? Is it enough that
    # ANY ASSET is a raster tile set?

    callback: Callback = callback_constructor(asset_id)

    job_list = []

    # For GLAD/RADD, create intensity asset with pixetl and merge with
    # existing date_conf layer to form a new asset
    if input_data["creation_options"]["use_intensity"]:

        # Get pixetl settings from the raster tile set asset's creation options
        # FIXME: Possible that the raster tile set is not the default asset?
        default_asset = await get_default_asset(dataset, version)
        date_conf_co = RasterTileSetSourceCreationOptions(
            **default_asset.creation_options
        ).dict(by_alias=True)
        print(f"BLAH BLAH DATE_CONF_CO: {json.dumps(date_conf_co, indent=2)}")

        layer_def = {
            "source_uri": [
                get_asset_uri(
                    dataset, version, AssetType.raster_tile_set, date_conf_co
                ).replace("{tile_id}.tif", "tiles.geojson")
            ],
            "source_type": "raster",
            "source_driver": date_conf_co["source_driver"],
            "data_type": date_conf_co["data_type"],
            "pixel_meaning": "intensity",
            "grid": date_conf_co["grid"],
            "resampling": "med",
            "nbits": date_conf_co["nbits"],
            "no_data": date_conf_co["no_data"],
            "overwrite": True,
            "subset": date_conf_co["subset"],
            "calc": "(A>0)*55",
        }

        print(f"BLAH BLAH LAYER_DEF: {json.dumps(layer_def, indent=2)}")

        intensity_co = RasterTileSetSourceCreationOptions(**layer_def)

        asset_options = AssetCreateIn(
            asset_type=AssetType.raster_tile_set,
            asset_uri="http://www.slashdot.org",
            is_managed=True,
            creation_options=intensity_co,
            metadata={},
        ).dict(by_alias=True)

        # intensity_co = RasterTileSetSourceCreationOptions(**layer_def).dict(by_alias=True, exclude_none=True)

        # print(f"BLAH BLAH INTENSITY_CO: {json.dumps(intensity_co, indent=2)}")

        # intensity_co = copy.deepcopy(date_conf_co)
        # intensity_co["source_uri"] = get_asset_uri(dataset, version, AssetType.raster_tile_set, date_conf_co)
        # intensity_co["pixel_meaning"] = "intensity"
        # intensity_co["calc"] = "(A>0)*55"
        # print(f"BLAH BLAH INTENSITY_CO: {json.dumps(intensity_co, indent=2)}")
        intensity_asset = await create_asset(dataset, version, **asset_options)
        print("INTENSITY ASSET CREATED")

        intensity_job = await _run_pixetl(
            dataset,
            version,
            intensity_asset.creation_options,
            "create_intensity",
            callback,
        )
        job_list.append(intensity_job)
        print("INTENSITY JOB CREATED")

        # Re-project date_conf and intensity to web mercator with pixetl

        # tile_sets = get_assets_by_filter(
        #     dataset=dataset, version=version, asset_types=[AssetType.raster_tile_set]
        # )
        #
        date_conf_uri = get_asset_uri(
            dataset, version, AssetType.raster_tile_set, date_conf_co
        ).replace("{tile_id}.tif", "tiles.geojson")
        # for zoom_level in range(min_zoom, max_static_zoom):
        zoom_level = 0
        co = copy.deepcopy(date_conf_co)
        # s_d = co.pop("source_driver")
        # s_t = co.pop("source_type")
        # s_u = co.pop("source_uri")
        co["source_uri"] = [date_conf_uri]
        co["calc"] = None
        co["grid"] = f"zoom_{zoom_level}"
        co["resampling"] = "med"
        co["overwrite"] = True
        wm_co = RasterTileSetSourceCreationOptions(**co)

        asset_options = AssetCreateIn(
            asset_type=AssetType.raster_tile_set,
            asset_uri="http://www.aclu.org",
            is_managed=True,
            creation_options=wm_co,
            metadata={},
        ).dict(by_alias=True)
        wm_asset = await create_asset(dataset, version, **asset_options)
        print(f"DATE_CONF ZOOM LEVEL {zoom_level} ASSET CREATED")

        callback = callback_constructor(wm_asset.asset_id)
        zoom_level_job = await _run_pixetl(
            dataset,
            version,
            wm_asset.creation_options,
            f"date_conf_zoom_{zoom_level}_reprojection",
            callback,
            # parents=[str(intensity_job)]
        )
        job_list.append(zoom_level_job)
        print(f"DATE_CONF ZOOM LEVEL {zoom_level} JOB CREATED")

        # intensity_uri = get_asset_uri(dataset, version, AssetType.raster_tile_set, intensity_asset.creation_options).replace("{tile_id}.tif", "tiles.geojson")
        # # for zoom_level in range(min_zoom, max_static_zoom):
        # co = copy.deepcopy(intensity_asset.creation_options)
        # co["source_uri"] = [intensity_uri]
        # co["calc"] = None
        # co["grid"] = f"zoom_{zoom_level}"
        # co["resampling"] = "med"
        # co["overwrite"] = True
        # co["subset"] = True
        # wm_co = RasterTileSetSourceCreationOptions(**co)
        # asset_options = AssetCreateIn(
        #     asset_type=AssetType.raster_tile_set,
        #     asset_uri="http://www.apple.com",
        #     is_managed=True,
        #     creation_options=wm_co,
        #     metadata={}
        # ).dict(by_alias=True)
        # wm_asset = await create_asset(dataset, version, **asset_options)
        # callback: Callback = callback_constructor(wm_asset.asset_id)
        # zoom_level_job = await _run_pixetl(
        #     dataset,
        #     version,
        #     co,
        #     f"intensity_zoom_{zoom_level}_reprojection",
        #     callback,
        #     parents=[str(intensity_job)]
        # )
        # job_list.append(zoom_level_job)

        # # Merge intensity and date_conf into a single asset using build_rgb
        # for zoom_level in range(min_zoom, max_static_zoom):
        # # FIXME: Hard-coding these for the moment:
        # # srid: str = "epsg-3857"
        # srid: str = "epsg-4326"
        # # grid: str = "90/27008"
        # grid: str = "90/27008"
        # date_conf_uri: str = f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/raster/{srid}/{grid}/date_conf/geotiff/tiles.geojson"
        # intensity_uri: str = f"s3://{DATA_LAKE_BUCKET}/{dataset}/{version}/raster/{srid}/{grid}/intensity/geotiff/tiles.geojson"
        #
        # command = [
        #     "merge_intensity.sh",
        #     "-d",
        #     dataset,
        #     "-v",
        #     version,
        #     date_conf_uri,
        #     intensity_uri,
        # ]
        #
        # merge_intensity_job = BuildRGBJob(
        #     job_name="merge_intensity_and_date_conf_assets",
        #     command=command,
        #     environment=job_env,
        #     parents=[str(intensity_job)],
        #     callback=callback,
        # )
        # job_list.append(merge_intensity_job)

        # FIXME: build_rgb created the merged tiles but not tiles.geojson or extent.geojson
        # Create those now with pixetl's source_prep?

        print("Yup, made it here")
        print(f"JOB LIST LENGTH SO FAR: {len(job_list)}")
        for job in job_list:
            print(f"JOB: {job.job_name}")
            print(f"  PARENTS: {job.parents}")

    # Actually create the tile cache using gdal2tiles
    print("Now create the tile cache using gdal2tiles...")

    log: ChangeLog = await execute(job_list)

    return log
