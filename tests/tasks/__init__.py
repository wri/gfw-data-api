from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy.sql.ddl import CreateSchema

from app.application import ContextEngine, db
from app.crud import assets, datasets, versions
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
            dataset, version, asset_type=asset_type, asset_uri=asset_uri,
        )
    return new_asset
