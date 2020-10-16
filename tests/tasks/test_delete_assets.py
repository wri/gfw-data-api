import pytest

from app.application import ContextEngine, db
from app.settings.globals import DATA_LAKE_BUCKET
from app.tasks.delete_assets import (
    delete_database_table_asset,
    delete_raster_tileset_assets,
)
from app.utils.aws import get_s3_client
from tests import TSV_PATH


@pytest.mark.asyncio
async def test_delete_raster_tileset_assets():
    s3_client = get_s3_client()
    dataset = "test_delete_raster_tileset"
    version = "table"
    srid = "epsg-4326"
    grid = "10/40000"
    value = "year"

    for i in range(0, 10):
        s3_client.upload_file(
            TSV_PATH,
            DATA_LAKE_BUCKET,
            f"{dataset}/{version}/raster/{srid}/{grid}/{value}/test_{i}.tsv",
        )

    response = s3_client.list_objects_v2(Bucket=DATA_LAKE_BUCKET, Prefix=dataset)

    assert response["KeyCount"] == 10

    await delete_raster_tileset_assets(dataset, version, srid, grid, value)

    response = s3_client.list_objects_v2(Bucket=DATA_LAKE_BUCKET, Prefix=dataset)
    assert response["KeyCount"] == 0


@pytest.mark.asyncio
async def test_delete_database_table():
    dataset = "test"
    version = "table"

    async with ContextEngine("WRITE"):
        # create schema and stable
        await db.all(f"CREATE SCHEMA {dataset};")
        await db.all(f"CREATE TABLE {dataset}.{version} (col1 text);")

        rows = await db.all(f"select * from pg_tables where schemaname='{dataset}';")
        assert len(rows) == 1

        # test if function drops table
        await delete_database_table_asset(dataset, version)

        rows = await db.all(f"select * from pg_tables where schemaname='{dataset}';")
        assert len(rows) == 0

        # clean up
        await db.all(f"DROP SCHEMA {dataset};")
