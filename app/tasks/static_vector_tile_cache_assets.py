import io
import json
from typing import Any, Dict, List, Optional
from uuid import UUID

from ..crud import assets, metadata
from ..errors import RecordNotFoundError
from ..models.orm.assets import Asset as ORMAsset
from ..models.orm.dataset_metadata import DatasetMetadata as ORMDatasetMetadata
from ..models.pydantic.assets import AssetType
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import (
    StaticVectorTileCacheCreationOptions,
    creation_option_factory,
)
from ..models.pydantic.jobs import GdalPythonExportJob, TileCacheJob
from ..settings.globals import TILE_CACHE_BUCKET, TILE_CACHE_JOB_QUEUE, TILE_CACHE_URL
from ..utils.aws import get_s3_client
from ..utils.fields import get_field_attributes
from ..utils.path import get_asset_uri
from . import callback_constructor, reader_secrets, report_vars
from .batch import execute


async def static_vector_tile_cache_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:
    """Create Vector tile cache and NDJSON file as intermediate data."""

    #######################
    # Update asset metadata
    #######################

    creation_options = creation_option_factory(
        AssetType.static_vector_tile_cache, input_data["creation_options"]
    )

    field_attributes: List[Dict[str, Any]] = await get_field_attributes(
        dataset, version, creation_options
    )

    await assets.update_asset(
        asset_id,
        metadata={
            "min_zoom": creation_options.min_zoom,
            "max_zoom": creation_options.max_zoom,
        },
        fields=field_attributes,
    )

    ############################
    # Create NDJSON asset as side effect
    ############################

    ndjson_uri = get_asset_uri(dataset, version, AssetType.ndjson)

    ndjson_asset: ORMAsset = await assets.create_asset(
        dataset,
        version,
        asset_type=AssetType.ndjson,
        asset_uri=ndjson_uri,
        fields=field_attributes,
    )

    ############################
    # Define jobs
    ############################

    # Create table schema
    command: List[str] = [
        "export_vector_data.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-f",
        f"{dataset}_{version}.ndjson",
        "-F",
        "GeoJSONSeq",
        "-T",
        ndjson_uri,
        "-C",
        ",".join([field["name"] for field in field_attributes]),
    ]

    export_ndjson = GdalPythonExportJob(
        dataset=dataset,
        job_name="export_ndjson",
        job_queue=TILE_CACHE_JOB_QUEUE,
        command=command,
        environment=reader_secrets,
        callback=callback_constructor(ndjson_asset.asset_id),
    )

    command = [
        "create_vector_tile_cache.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-s",
        ndjson_uri,
        "-Z",
        str(creation_options.min_zoom),
        "-z",
        str(creation_options.max_zoom),
        "-t",
        creation_options.tile_strategy,
        "-I",
        creation_options.implementation,
    ]

    create_vector_tile_cache = TileCacheJob(
        dataset=dataset,
        job_name="create_vector_tile_cache",
        command=command,
        parents=[export_ndjson.job_name],
        environment=report_vars,
        callback=callback_constructor(asset_id),
    )

    #######################
    # execute jobs
    #######################

    log: ChangeLog = await execute([export_ndjson, create_vector_tile_cache])

    ######################
    # Generate ESRI Vector Tile Cache Server and root.json
    ######################

    root_template = _get_vector_tile_root_json(dataset, version, creation_options)
    tile_server_template = await _get_vector_tile_server(
        dataset, version, asset_id, creation_options
    )

    root_file = io.BytesIO(json.dumps(root_template).encode("utf-8"))
    tile_server_file = io.BytesIO(json.dumps(tile_server_template).encode("utf-8"))
    args = {"ContentType": "application/json", "CacheControl": "max-age=31536000"}

    client = get_s3_client()
    client.upload_fileobj(
        root_file,
        TILE_CACHE_BUCKET,
        f"{dataset}/{version}/{creation_options.implementation}/root.json",
        ExtraArgs=args,
    )
    client.upload_fileobj(
        tile_server_file,
        TILE_CACHE_BUCKET,
        f"{dataset}/{version}/{creation_options.implementation}/VectorTileServer",
        ExtraArgs=args,
    )

    return log


def _get_vector_tile_root_json(
    dataset: str, version: str, creation_options: StaticVectorTileCacheCreationOptions
) -> Dict[str, Any]:
    root_template = {
        "version": 8,
        "sources": {
            f"{dataset}": {
                "type": "vector",
                "url": f"{TILE_CACHE_URL}/{dataset}/{version}/{creation_options.implementation}/VectorTileServer",
            }
        },
        "layers": creation_options.layer_style,
    }
    return root_template


async def _get_vector_tile_server(
    dataset: str,
    version: str,
    asset_id: UUID,
    creation_options: StaticVectorTileCacheCreationOptions,
) -> Dict[str, Any]:

    try:
        dataset_metadata: Optional[
            ORMDatasetMetadata
        ] = await metadata.get_dataset_metadata(dataset)
    except RecordNotFoundError:
        dataset_metadata = None

    resolution = 78271.51696401172
    scale = 295829355.45453244
    _min = -20037508.342787
    _max = 20037508.342787
    spatial_reference = {"wkid": 102100, "latestWkid": 3857}
    extent = {
        "xmin": _min,
        "ymin": _min,
        "xmax": _max,
        "ymax": _max,
        "spatialReference": spatial_reference,
    }

    response = {
        "currentVersion": 10.7,
        "name": dataset_metadata.title if dataset_metadata else "",
        "copyrightText": dataset_metadata.citation if dataset_metadata else "",
        "capabilities": "TilesOnly",
        "type": "indexedVector",
        "defaultStyles": "resources/styles",
        "tiles": [
            f"{TILE_CACHE_URL}/{dataset}/{version}/{creation_options.implementation}/{{z}}/{{x}}/{{y}}.pbf"
        ],
        "exportTilesAllowed": False,
        "initialExtent": extent,
        "fullExtent": extent,
        "minScale": 0,
        "maxScale": 0,
        "tileInfo": {
            "rows": 512,
            "cols": 512,
            "dpi": 96,
            "format": "pbf",
            "origin": {"x": _min, "y": _max},
            "spatialReference": spatial_reference,
            "lods": [
                {
                    "level": i,
                    "resolution": resolution / (2**i),
                    "scale": scale / (2**i),
                }
                for i in range(creation_options.min_zoom, creation_options.max_zoom + 1)
            ],
        },
        "maxzoom": 22,
        "minLOD": creation_options.min_zoom,
        "maxLOD": creation_options.max_zoom,
        "resourceInfo": {
            "styleVersion": 8,
            "tileCompression": "gzip",
            "cacheInfo": {
                "storageInfo": {"packetSize": 128, "storageFormat": "compactV2"}
            },
        },
        "serviceItemId": str(asset_id),
    }
    return response
