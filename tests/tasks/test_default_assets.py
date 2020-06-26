import os
from typing import List
from uuid import UUID

import boto3
import pendulum
import pytest
from pendulum.parsing.exceptions import ParserError
from sqlalchemy.sql.ddl import CreateSchema

from app.application import ContextEngine, db
from app.crud import assets, datasets, versions
from app.models.orm.assets import Asset
from app.models.orm.geostore import Geostore
from app.settings.globals import AWS_REGION, READER_USERNAME
from app.tasks.default_assets import create_default_asset
from app.utils.aws import get_s3_client

GEOJSON_NAME = "test.geojson"
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", GEOJSON_NAME)

TSV_NAME = "test.tsv"
TSV_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", TSV_NAME)

BUCKET = "test-bucket"


@pytest.mark.skip(reason="Needs to be updated for new task behavior")
@pytest.mark.asyncio
async def test_vector_source_asset(batch_client):

    _, logs = batch_client

    # Upload file to mocked S3 bucket
    s3_client = boto3.client(
        "s3", region_name=AWS_REGION, endpoint_url="http://motoserver:5000"
    )

    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.upload_file(GEOJSON_PATH, BUCKET, GEOJSON_NAME)

    dataset = "test"
    version = "v1.1.1"
    input_data = {
        "source_type": "vector",
        "source_uri": [f"s3://{BUCKET}/{GEOJSON_NAME}"],
        "creation_options": {"src_driver": "GeoJSON", "zipped": False},
        "metadata": {},
    }

    # Create dataset and version records
    async with ContextEngine("WRITE"):
        await datasets.create_dataset(dataset)
        await db.status(CreateSchema(dataset))
        await db.status(f"GRANT USAGE ON SCHEMA {dataset} TO {READER_USERNAME};")
        await db.status(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {dataset} GRANT SELECT ON TABLES TO {READER_USERNAME};"
        )
        await versions.create_version(dataset, version, **input_data)

    # To start off, version should be in status "pending"
    # and changelog should be an empty list
    row = await versions.get_version(dataset, version)
    assert row.status == "pending"
    assert row.change_log == []

    # Create default asset in mocked BATCH
    async with ContextEngine("WRITE"):
        await create_default_asset(dataset, version, input_data, None)

    # Get the logs in case something went wrong
    _print_logs(logs)

    # If everything worked, version should be set to "saved"
    # and there should now be a changelog item
    row = await versions.get_version(dataset, version)
    assert row.status == "saved"
    assert len(row.change_log) == 1
    print(f"VECTOR_SOURCE_VERSION LOGS: {row.change_log}")
    assert row.change_log[0]["message"] == "Successfully ran all batch jobs"

    # There should be a table called "test"."v1.1.1" with one row
    async with ContextEngine("READ"):
        count = await db.scalar(db.text('SELECT count(*) FROM test."v1.1.1"'))
    assert count == 1

    # The geometry should also be accessible via geostore
    async with ContextEngine("READ"):
        rows: List[Geostore] = await Geostore.query.gino.all()

    assert len(rows) == 1
    assert rows[0].gfw_geostore_id == UUID("b9faa657-34c9-96d4-fce4-8bb8a1507cb3")

    asset_rows: List[Asset] = await assets.get_assets(dataset, version)
    print(f"VECTOR SOURCE ASSET LOGS: {asset_rows[0].change_log}")
    assert len(asset_rows) == 1
    assert asset_rows[0].change_log[-1]["message"] == (
        "Successfully completed all scheduled batch jobs for asset creation"
    )
    assert len(asset_rows[0].change_log) == 15  # 14 for jobs, 1 for summary


@pytest.mark.skip(reason="Needs to be updated for new task behavior")
@pytest.mark.asyncio
async def test_table_source_asset(client, batch_client):
    _, logs = batch_client

    # test environment uses moto server
    s3_client = get_s3_client()

    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.upload_file(TSV_PATH, BUCKET, TSV_NAME)

    dataset = "table_test"
    version = "v202002.1"

    # define partition schema
    partition_schema = list()
    years = range(2018, 2021)
    for year in years:
        for week in range(1, 54):
            try:
                name = f"y{year}_w{week:02}"
                start = pendulum.parse(f"{year}-W{week:02}").to_date_string()
                end = pendulum.parse(f"{year}-W{week:02}").add(days=7).to_date_string()
                partition_schema.append(
                    {"partition_suffix": name, "start_value": start, "end_value": end}
                )

            except ParserError:
                # Year has only 52 weeks
                pass

    input_data = {
        "source_type": "table",
        "source_uri": [f"s3://{BUCKET}/{TSV_NAME}"],
        "creation_options": {
            "src_driver": "text",
            "delimiter": "\t",
            "has_header": True,
            "latitude": "latitude",
            "longitude": "longitude",
            "cluster": {"index_type": "gist", "column_name": "geom_wm"},
            "partitions": {
                "partition_type": "range",
                "partition_column": "alert__date",
                "partition_schema": partition_schema,
            },
            "indices": [
                {"index_type": "gist", "column_name": "geom"},
                {"index_type": "gist", "column_name": "geom_wm"},
                {"index_type": "btree", "column_name": "alert__date"},
            ],
            "table_schema": [
                {
                    "field_name": "rspo_oil_palm__certification_status",
                    "field_type": "text",
                },
                {"field_name": "per_forest_concession__type", "field_type": "text"},
                {"field_name": "idn_forest_area__type", "field_type": "text"},
                {"field_name": "alert__count", "field_type": "integer"},
                {"field_name": "adm1", "field_type": "integer"},
                {"field_name": "adm2", "field_type": "integer"},
            ],
        },
        "metadata": {},
    }

    # Create dataset and version records
    async with ContextEngine("WRITE"):
        await datasets.create_dataset(dataset)
        await db.status(CreateSchema(dataset))
        await db.status(f"GRANT USAGE ON SCHEMA {dataset} TO {READER_USERNAME};")
        await db.status(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {dataset} GRANT SELECT ON TABLES TO {READER_USERNAME};"
        )
        await versions.create_version(dataset, version, **input_data)

    # To start off, version should be in status "pending"
    row = await versions.get_version(dataset, version)
    assert row.status == "pending"

    # Create default asset in mocked BATCH
    async with ContextEngine("WRITE"):
        await create_default_asset(
            dataset, version, input_data, None,
        )

    # Get the logs in case something went wrong
    _print_logs(logs)

    # If everything worked, version should be set to "saved"
    # and there should now be a changelog item
    row = await versions.get_version(dataset, version)
    assert row.status == "saved"
    assert len(row.change_log) == 1
    print(f"TABLE SOURCE VERSION LOGS: {row.change_log}")
    assert row.change_log[0]["message"] == "Successfully ran all batch jobs"

    rows = await assets.get_assets(dataset, version)
    assert len(rows) == 1
    print(rows[0].metadata)
    assert rows[0].status == "saved"
    assert len(rows[0].metadata["fields_"]) == 33
    assert rows[0].is_default is True

    asset_rows: List[Asset] = await assets.get_assets(dataset, version)
    print(f"TABLE SOURCE ASSET LOGS: {asset_rows[0].change_log}")
    assert len(asset_rows) == 1
    assert asset_rows[0].change_log[-1]["message"] == (
        "Successfully completed all scheduled batch jobs for asset creation"
    )
    assert len(asset_rows[0].change_log) == 17  # 16 for jobs, 1 for summary

    _assert_fields(
        rows[0].metadata["fields_"], input_data["creation_options"]["table_schema"]
    )

    # There should be a table called "table_test"."v202002.1" with 99 rows.
    # It should have the right amount of partitions and indices
    async with ContextEngine("READ"):
        count = await db.scalar(
            db.text(
                f"""
                    SELECT count(*)
                        FROM "{dataset}"."{version}";"""
            )
        )
        partition_count = await db.scalar(
            db.text(
                f"""
                    SELECT count(i.inhrelid::regclass)
                        FROM pg_inherits i
                        WHERE  i.inhparent = '"{dataset}"."{version}"'::regclass;"""
            )
        )
        index_count = await db.scalar(
            db.text(
                f"""
                    SELECT count(indexname)
                        FROM pg_indexes
                        WHERE schemaname = '{dataset}' AND tablename like '{version}%';"""
            )
        )
        cluster_count = await db.scalar(
            db.text(
                """
                    SELECT count(relname)
                        FROM   pg_class c
                        JOIN   pg_index i ON i.indrelid = c.oid
                        WHERE  relkind = 'r' AND relhasindex AND i.indisclustered"""
            )
        )

    assert count == 99
    assert partition_count == len(partition_schema)
    assert index_count == partition_count * len(
        input_data["creation_options"]["indices"]
    )
    assert cluster_count == len(partition_schema)


def _assert_fields(field_list, field_schema):
    count = 0
    for field in field_list:
        for schema in field_schema:
            if (
                field["field_name_"] == schema["field_name"]
                and field["field_type"] == schema["field_type"]
            ):
                count += 1
        if field["field_name_"] in ["geom", "geom_wm", "gfw_geojson", "gfw_bbox"]:
            assert not field["is_filter"]
            assert not field["is_feature_info"]
        else:
            assert field["is_filter"]
            assert field["is_feature_info"]
    assert count == len(field_schema)


def _print_logs(logs):
    resp = logs.describe_log_streams(logGroupName="/aws/batch/job")

    for stream in resp["logStreams"]:
        ls_name = stream["logStreamName"]

        stream_resp = logs.get_log_events(
            logGroupName="/aws/batch/job", logStreamName=ls_name
        )

        print(f"-------- LOGS FROM {ls_name} --------")
        for event in stream_resp["events"]:
            print(event["message"])
