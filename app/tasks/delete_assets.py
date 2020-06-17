from ..application import ContextEngine
from ..settings.globals import (
    DATA_LAKE_BUCKET,
    TILE_CACHE_BUCKET,
    TILE_CACHE_CLOUDFRONT_ID,
)
from .aws_tasks import expire_s3_objects, flush_cloudfront_cache


async def delete_all_assets(dataset: str, version: str) -> None:
    await _delete_database_table(dataset, version)
    expire_s3_objects(DATA_LAKE_BUCKET, f"{dataset}/{version}/")
    expire_s3_objects(TILE_CACHE_BUCKET, f"{dataset}/{version}/")
    flush_cloudfront_cache(TILE_CACHE_CLOUDFRONT_ID, f"{dataset}/{version}/*")


async def delete_dynamic_vector_tile_cache_assets(
    dataset: str, version: str, implementation: str = "dynamic"
) -> None:
    flush_cloudfront_cache(
        TILE_CACHE_CLOUDFRONT_ID, f"{dataset}/{version}/{implementation}/*"
    )


async def delete_static_vector_tile_cache_assets(dataset: str, version: str) -> None:
    expire_s3_objects(TILE_CACHE_BUCKET, f"{dataset}/{version}/default/")
    flush_cloudfront_cache(TILE_CACHE_CLOUDFRONT_ID, f"{dataset}/{version}/default/*")


async def delete_raster_tileset_assets(
    dataset: str, version: str, grid: str, value: str
) -> None:
    expire_s3_objects(DATA_LAKE_BUCKET, f"{dataset}/{version}/raster/{grid}/{value}")


async def _delete_database_table(dataset, version):
    async with ContextEngine("PUT") as db:
        await db.status(f"""DROP TABLE IF EXISTS "{dataset}"."{version}" CASCADE;""")
