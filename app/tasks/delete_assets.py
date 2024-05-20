from ..application import ContextEngine, db
from ..settings.globals import (
    DATA_LAKE_BUCKET,
    TILE_CACHE_BUCKET,
    TILE_CACHE_CLOUDFRONT_ID,
)
from ..utils.path import split_s3_path
from .aws_tasks import delete_s3_objects, expire_s3_objects, flush_cloudfront_cache


async def delete_all_assets(dataset: str, version: str) -> None:
    await delete_database_table_asset(dataset, version)
    delete_s3_objects(DATA_LAKE_BUCKET, f"{dataset}/{version}/")
    # expire_s3_objects(TILE_CACHE_BUCKET, f"{dataset}/{version}/")
    flush_cloudfront_cache(TILE_CACHE_CLOUDFRONT_ID, [f"/{dataset}/{version}/*"])


async def delete_dynamic_vector_tile_cache_assets(
    dataset: str, version: str, implementation: str = "dynamic"
) -> None:
    flush_cloudfront_cache(
        TILE_CACHE_CLOUDFRONT_ID, [f"{dataset}/{version}/{implementation}/*"]
    )


async def delete_static_vector_tile_cache_assets(
    dataset: str, version: str, implementation: str = "default"
) -> None:
    expire_s3_objects(
        TILE_CACHE_BUCKET, f"{dataset}/{version}/{implementation}/", "format", "pbf"
    )
    flush_cloudfront_cache(
        TILE_CACHE_CLOUDFRONT_ID, [f"{dataset}/{version}/{implementation}/*.pbf"]
    )


async def delete_raster_tile_cache_assets(
    dataset: str, version: str, implementation: str = "default"
) -> None:
    expire_s3_objects(
        TILE_CACHE_BUCKET, f"{dataset}/{version}/{implementation}/", "format", "png"
    )
    flush_cloudfront_cache(
        TILE_CACHE_CLOUDFRONT_ID, [f"{dataset}/{version}/{implementation}/*.png"]
    )


async def delete_raster_tileset_assets(
    dataset: str,
    version: str,
    srid: str,
    grid: str,
    value: str,
) -> None:
    delete_s3_objects(
        DATA_LAKE_BUCKET, f"{dataset}/{version}/raster/{srid}/{grid}/{value}/"
    )


async def delete_database_table_asset(dataset: str, version: str) -> None:
    async with ContextEngine("WRITE"):
        await db.status(f"""DROP TABLE IF EXISTS "{dataset}"."{version}" CASCADE;""")


async def delete_single_file_asset(uri: str):
    bucket, key = split_s3_path(uri)
    delete_s3_objects(bucket, key)
