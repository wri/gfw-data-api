import json
from collections import defaultdict
from copy import deepcopy
from typing import Any, DefaultDict, Dict, List, Optional
from uuid import UUID

from fastapi import HTTPException

from app.crud.assets import get_asset, get_default_asset, update_asset
from app.errors import RecordNotFoundError
from app.models.enum.assets import AssetStatus, AssetType
from app.models.enum.creation_options import RasterDrivers
from app.models.enum.sources import RasterSourceType, VectorSourceType
from app.models.orm.assets import Asset as ORMAsset
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import PixETLCreationOptions
from app.models.pydantic.extent import Extent
from app.models.pydantic.geostore import FeatureCollection
from app.models.pydantic.jobs import Job
from app.models.pydantic.statistics import BandStats, Histogram, RasterStats
from app.tasks import Callback, callback_constructor
from app.tasks.batch import execute
from app.tasks.raster_tile_set_assets.utils import create_pixetl_job
from app.utils.aws import get_s3_client
from app.utils.path import (
    get_asset_uri,
    infer_srid_from_grid,
    split_s3_path,
    tile_uri_to_extent_geojson,
    tile_uri_to_tiles_geojson,
)
from app.utils.stats import merge_n_histograms


async def raster_tile_set_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:

    # If being created as a source (default) asset, creation_options["source_uri"]
    # will be a list. When being created as an auxiliary asset, it will be None.
    # In the latter case we will generate one for pixETL based on the default asset,
    # below.

    co = deepcopy(input_data["creation_options"])

    source_uris: Optional[List[str]] = co.get("source_uri")
    if source_uris is None:
        default_asset: ORMAsset = await get_default_asset(dataset, version)

        if default_asset.creation_options["source_type"] == RasterSourceType.raster:
            co["source_type"] = RasterSourceType.raster
            co["source_uri"] = [tile_uri_to_tiles_geojson(default_asset.asset_uri)]
            co["source_driver"] = RasterDrivers.geotiff
            auxiliary_assets = co.pop("auxiliary_assets", None)
            if auxiliary_assets:
                for aux_asset_id in auxiliary_assets:
                    auxiliary_asset: ORMAsset = await get_asset(aux_asset_id)
                    co["source_uri"].append(
                        tile_uri_to_tiles_geojson(auxiliary_asset.asset_uri)
                    )

        elif default_asset.creation_options["source_type"] == VectorSourceType.vector:
            co["source_type"] = VectorSourceType.vector

    creation_options = PixETLCreationOptions(**co)

    callback: Callback = callback_constructor(asset_id)

    create_raster_tile_set_job: Job = await create_pixetl_job(
        dataset, version, creation_options, "create_raster_tile_set", callback
    )

    log: ChangeLog = await execute([create_raster_tile_set_job])

    return log


async def get_extent(asset_id: UUID) -> Optional[Extent]:
    asset_row: ORMAsset = await get_asset(asset_id)
    asset_uri: str = get_asset_uri(
        asset_row.dataset,
        asset_row.version,
        asset_row.asset_type,
        asset_row.creation_options,
        srid=infer_srid_from_grid(asset_row.creation_options.get("grid")),
    )
    bucket, key = split_s3_path(tile_uri_to_extent_geojson(asset_uri))

    s3_client = get_s3_client()
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    extent_geojson: Dict[str, Any] = json.loads(resp["Body"].read().decode("utf-8"))

    if extent_geojson:
        return Extent(**extent_geojson)
    return None


def _collect_bandstats(fc: FeatureCollection) -> List[BandStats]:
    stats_by_band: DefaultDict[int, DefaultDict[str, List[int]]] = defaultdict(
        lambda: defaultdict(lambda: [])
    )
    histograms_by_band: DefaultDict[int, List[Histogram]] = defaultdict(lambda: [])

    for f_i, feature in enumerate(fc.features):
        for i, band in enumerate(feature.properties.get("bands", list())):
            if band.get("stats") is not None:
                for val in ("min", "max", "mean"):
                    stats_by_band[i][val].append(band["stats"][val])

            feature_histo_dict_i = band.get("histogram")
            if feature_histo_dict_i is not None:
                histo_i = Histogram(
                    min=feature_histo_dict_i["min"],
                    max=feature_histo_dict_i["max"],
                    bin_count=feature_histo_dict_i["count"],
                    value_count=feature_histo_dict_i["buckets"],
                )
                histograms_by_band[i].append(histo_i)

    bandstats: List[BandStats] = []
    for i, band in stats_by_band.items():
        bs = BandStats(
            min=min(stats_by_band[i]["min"]),
            max=max(stats_by_band[i]["max"]),
            mean=sum(stats_by_band[i]["mean"]) / len(stats_by_band[i]["mean"]),
        )
        bs.histogram = merge_n_histograms(histograms_by_band[i])
        bandstats.append(bs)

    return bandstats


async def _get_raster_stats(asset_id: UUID) -> RasterStats:
    asset_row: ORMAsset = await get_asset(asset_id)

    asset_uri: str = get_asset_uri(
        asset_row.dataset,
        asset_row.version,
        asset_row.asset_type,
        asset_row.creation_options,
        srid=infer_srid_from_grid(asset_row.creation_options.get("grid")),
    )
    bucket, tiles_key = split_s3_path(tile_uri_to_tiles_geojson(asset_uri))

    s3_client = get_s3_client()
    tiles_resp = s3_client.get_object(Bucket=bucket, Key=tiles_key)
    tiles_geojson: Dict[str, Any] = json.loads(
        tiles_resp["Body"].read().decode("utf-8")
    )

    bandstats: List[BandStats] = _collect_bandstats(FeatureCollection(**tiles_geojson))

    return RasterStats(bands=bandstats)


async def raster_tile_set_validator(
    dataset: str, version: str, input_data: Dict[str, Any]
) -> None:
    """Validate Raster Tile Cache Creation Options.

    Used in asset route. If validation fails, it will raise a
    HTTPException visible to user.
    """
    for asset_id in input_data["creation_options"]["auxiliary_assets"]:

        try:
            auxiliary_asset: ORMAsset = await get_asset(asset_id)
        except RecordNotFoundError:
            raise HTTPException(
                status_code=400,
                detail=f"Auxiliary asset {asset_id} does not exist.",
            )
        if auxiliary_asset.status != AssetStatus.saved:
            raise HTTPException(
                status_code=400,
                detail=f"Auxiliary asset {asset_id} not in status {AssetStatus.saved}.",
            )
        if auxiliary_asset.asset_type != AssetType.raster_tile_set:
            raise HTTPException(
                status_code=400,
                detail=f"Auxiliary asset {asset_id} not a {AssetType.raster_tile_set}.",
            )
        if (
            auxiliary_asset.creation_options["grid"]
            != input_data["creation_options"]["grid"]
        ):
            raise HTTPException(
                status_code=400,
                detail=f"Input grid and grid of auxiliary asset {asset_id} do not match.",
            )


async def raster_tile_set_post_completion_task(asset_id: UUID):
    extent: Optional[Extent] = await get_extent(asset_id)

    stats: Optional[RasterStats] = None

    asset_row: ORMAsset = await get_asset(asset_id)
    if asset_row.creation_options["compute_stats"]:
        stats = await _get_raster_stats(asset_id)

    _: ORMAsset = await update_asset(asset_id, extent=extent, stats=stats)
