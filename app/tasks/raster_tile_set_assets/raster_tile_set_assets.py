import json
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional
from uuid import UUID

from app.crud.assets import get_asset, get_default_asset, update_asset
from app.models.orm.assets import Asset as ORMAsset
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import (
    RasterTileSetAssetCreationOptions,
    RasterTileSetSourceCreationOptions,
)
from app.models.pydantic.statistics import BandStats, RasterStats
from app.tasks import Callback, callback_constructor
from app.tasks.batch import execute
from app.tasks.raster_tile_set_assets.utils import create_pixetl_job
from app.utils.aws import get_s3_client
from app.utils.path import (
    get_asset_uri,
    infer_srid_from_grid,
    split_s3_path,
    tile_uri_to_tiles_geojson,
)


async def raster_tile_set_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:

    # If being created as a source (default) asset, creation_options["source_uri"]
    # will be a list. When being created as an auxiliary asset, it will be None.
    # In the latter case we will generate one for pixETL based on the default asset,
    # below.
    source_uris: Optional[List[str]] = input_data["creation_options"].get("source_uri")
    if source_uris is None:
        creation_options = RasterTileSetAssetCreationOptions(
            **input_data["creation_options"]
        ).dict(exclude_none=True, by_alias=True)

        default_asset = await get_default_asset(dataset, version)

        if default_asset.creation_options["source_type"] == "raster":
            creation_options["source_type"] = "raster"
            creation_options["source_uri"] = default_asset.creation_options[
                "source_uri"
            ]
        elif default_asset.creation_options["source_type"] == "vector":
            creation_options["source_type"] = "vector"
    else:
        # FIXME move to validator function and assess prior to running background task
        if len(source_uris) > 1:
            raise AssertionError("Raster assets currently only support one input file")
        elif len(source_uris) == 0:
            raise AssertionError("source_uri must contain a URI to an input file in S3")
        creation_options = RasterTileSetSourceCreationOptions(
            **input_data["creation_options"]
        ).dict(exclude_none=True, by_alias=True)
        creation_options["source_uri"] = source_uris

    callback: Callback = callback_constructor(asset_id)

    create_raster_tile_set_job = await create_pixetl_job(
        dataset, version, creation_options, "create_raster_tile_set", callback
    )

    log: ChangeLog = await execute([create_raster_tile_set_job])

    return log


async def raster_tile_set_post_completion_task(asset_id: UUID):
    asset_row: ORMAsset = await get_asset(asset_id)

    if asset_row.creation_options["compute_stats"]:
        # Get tiles.geojson and extent.geojson from S3
        s3_client = get_s3_client()
        asset_uri = get_asset_uri(
            asset_row.dataset,
            asset_row.version,
            asset_row.asset_type,
            asset_row.creation_options,
            srid=infer_srid_from_grid(asset_row.creation_options.get("grid")),
        )
        # FIXME: Save extent
        # bucket, extent_key = split_s3_path(tile_uri_to_extent_geojson(asset_uri))
        # extent_resp = s3_client.get_object(Bucket=bucket, Key=extent_key)
        # extent_geojson: Dict = json.loads(
        #     extent_resp["Body"].read().decode("utf-8")
        # )

        bucket, tiles_key = split_s3_path(tile_uri_to_tiles_geojson(asset_uri))
        tiles_resp = s3_client.get_object(Bucket=bucket, Key=tiles_key)
        tiles_geojson: dict = json.loads(tiles_resp["Body"].read().decode("utf-8"))

        stats_by_band: DefaultDict[int, DefaultDict[str, List[int]]] = defaultdict(
            lambda: defaultdict(lambda: [])
        )
        histogram_by_band: Dict[int, Dict[str, Any]] = dict()

        for feature in tiles_geojson["features"]:
            for i, band in enumerate(feature["properties"]["bands"]):
                for val in ("min", "max", "mean"):
                    if band.get("stats", dict()).get(val) is not None:  # eliminate?
                        stats_by_band[i][val].append(band["stats"][val])

                histo = band["histogram"]
                if histogram_by_band.get(i) is None:
                    histogram_by_band[i] = {
                        "min": histo["min"],
                        "max": histo["max"],
                        "bin_count": histo["count"],
                        "value_count": histo["buckets"],
                    }
                else:
                    # len(histo["buckets"]) should == histo["count"]. Assert?
                    histogram_by_band[i]["min"] = min(
                        histogram_by_band[i]["min"], histo["min"]
                    )
                    histogram_by_band[i]["max"] = max(
                        histogram_by_band[i]["max"], histo["max"]
                    )
                    for bucket_num, bucket_val in enumerate(histo["buckets"]):
                        histogram_by_band[i]["value_count"][bucket_num] += bucket_val

        bandstats = []
        for i, band in stats_by_band.items():
            bandstats.append(
                BandStats(
                    min=min(stats_by_band[i]["min"]),
                    max=max(stats_by_band[i]["max"]),
                    mean=sum(stats_by_band[i]["mean"]) / len(stats_by_band[i]["mean"]),
                    histogram=histogram_by_band[i],
                )
            )

        _: ORMAsset = await update_asset(asset_id, stats=RasterStats(bands=bandstats))
