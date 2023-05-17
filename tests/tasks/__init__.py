from datetime import datetime
from typing import Any, Dict, List
from uuid import UUID

from sqlalchemy.sql.ddl import CreateSchema

from app.application import ContextEngine, db
from app.crud import assets, datasets, tasks, versions
from app.models.enum.assets import AssetStatus, AssetType
from app.models.orm.assets import Asset as AssetORM
from app.settings.globals import READER_USERNAME

KEY = "KEY"
VALUE = "VALUE"


class MockS3Client(object):
    rules: List[Dict[str, Any]] = []

    def get_bucket_lifecycle_configuration(self, Bucket):
        return {"Rules": self.rules}

    def put_bucket_lifecycle_configuration(self, Bucket, LifecycleConfiguration):
        self.rules = LifecycleConfiguration["Rules"]
        return {
            "ResponseMetadata": {"...": "..."},
        }


class MockCloudfrontClient(object):
    def create_invalidation(self, DistributionId, InvalidationBatch):
        return {
            "Location": "string",
            "Invalidation": {
                "Id": "string",
                "Status": "string",
                "CreateTime": datetime.now(),
                "InvalidationBatch": InvalidationBatch,
            },
        }


class MockECSClient(object):
    def update_service(self, cluster, service, forceNewDeployment):
        return {"service": {"serviceName": service}}


async def create_dataset(dataset) -> None:

    # Create dataset record and dataset schema
    async with ContextEngine("WRITE"):
        await datasets.create_dataset(dataset)
        await db.status(CreateSchema(dataset))
        await db.status(f"GRANT USAGE ON SCHEMA {dataset} TO {READER_USERNAME};")
        await db.status(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {dataset} GRANT SELECT ON TABLES TO {READER_USERNAME};"
        )
    row = await datasets.get_dataset(dataset)
    assert row.dataset == dataset
    assert dataset == await db.scalar(
        f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{dataset}';"
    )


async def create_version(dataset, version, input_data) -> None:
    # Create dataset and version records
    async with ContextEngine("WRITE"):
        await versions.create_version(dataset, version, **input_data)

    # Make sure everything we need is in place
    # To start off, version should be in status "pending"
    # and changelog should be an empty list
    # and dataset schema should exist
    row = await versions.get_version(dataset, version)
    assert row.status == "pending"
    assert row.change_log == []


async def create_asset(dataset, version, asset_type, asset_uri, input_data) -> AssetORM:
    # Create dataset and version records
    await create_dataset(dataset)
    await create_version(dataset, version, input_data)
    async with ContextEngine("WRITE"):
        new_asset = await assets.create_asset(
            dataset,
            version,
            asset_type=asset_type,
            asset_uri=asset_uri,
        )
    return new_asset


def assert_fields(field_list, field_schema):
    count = 0
    for field in field_list:
        for schema in field_schema:
            if (
                field["name"] == schema["name"]
                and field["data_type"] == schema["data_type"]
            ):
                count += 1
        if field["name"] in ["geom", "geom_wm", "gfw_geojson", "gfw_bbox"]:
            assert not field["is_filter"]
            assert not field["is_feature_info"]
        else:
            assert field["is_filter"]
            assert field["is_feature_info"]
    assert count == len(field_schema)


async def check_version_status(dataset, version, log_count):
    row = await versions.get_version(dataset, version)
    assert row.status == "saved"

    try:
        assert len(row.change_log) == log_count
    except AssertionError:
        print(f"Expected {log_count} changelog rows, observed {len(row.change_log)}")
        for cl in row.change_log:
            print(cl)
        raise
    assert row.change_log[0]["message"] == "Successfully scheduled batch jobs"


async def check_asset_status(dataset, version, nb_assets):
    rows = await assets.get_assets(dataset, version)
    try:
        assert len(rows) == nb_assets
    except AssertionError:
        print(f"Expected {nb_assets} asset rows, but found {len(rows)}")
        for row in rows:
            print(row)
        raise

    assert rows[0].status == "saved"
    assert rows[0].is_default is True


async def check_task_status(asset_id: UUID, nb_jobs: int, last_job_name: str):
    rows = await tasks.get_tasks(asset_id)
    try:
        assert len(rows) == nb_jobs
    except AssertionError:
        print(f"Expected {nb_jobs} jobs but found {len(rows)}:")
        for row in rows:
            print(f"{row.change_log[0]['message']}")
        raise

    for row in rows:
        # in this test we don't set the final asset status to saved or failed
        assert row.status == "pending"

    # in this test we only see the logs from background task, not from batch jobs
    try:
        assert rows[-1].change_log[0]["message"] == f"Scheduled job {last_job_name}"
    except AssertionError:
        print(f"Last task's changelog: {rows[-1].change_log[0]['message']}")
        raise


async def check_dynamic_vector_tile_cache_status(dataset, version):
    rows = await assets.get_assets_by_filter(dataset, version)
    asset_row = rows[0]

    # SHP files have one additional attribute (fid)
    if asset_row.version == "v1.1.0":
        assert len(asset_row.metadata.fields) == 10
    else:
        assert len(asset_row.metadata.fields) == 9

    assert len(rows) == 2
    geo_database, dynamic_vector_tile = rows
    assert geo_database.asset_type == AssetType.geo_database_table
    assert dynamic_vector_tile.asset_type == AssetType.dynamic_vector_tile_cache
    assert dynamic_vector_tile.status == AssetStatus.saved
    assert (
        geo_database.metadata.fields[0].name
        == dynamic_vector_tile.metadata.fields[0].name
    )
    assert (
        geo_database.metadata.fields[0].data_type
        == dynamic_vector_tile.metadata.fields[0].data_type
    )
    assert (
        geo_database.metadata.fields[1].name
        == dynamic_vector_tile.metadata.fields[1].name
    )
    assert (
        geo_database.metadata.fields[1].data_type
        == dynamic_vector_tile.metadata.fields[1].data_type
    )
