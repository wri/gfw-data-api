import os
from typing import List
from uuid import UUID

import boto3
import pendulum
import pytest
from pendulum.parsing.exceptions import ParserError
from sqlalchemy.sql.ddl import CreateSchema

from app.application import ContextEngine, db
from app.crud import datasets, versions
from app.models.orm.geostore import Geostore
from app.settings.globals import AWS_REGION, READER_USERNAME
from app.tasks.default_assets import create_default_asset

GEOJSON_NAME = "test.geojson"
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", GEOJSON_NAME)

TSV_NAME = "test.tsv"
TSV_PATH = os.path.join(os.path.dirname(__file__), "..", "fixtures", TSV_NAME)

BUCKET = "test-bucket"


@pytest.mark.asyncio
async def test_vector_source_asset(batch_client):
    # TODO: define what a callback should do
    async def callback(message):
        pass

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
    async with ContextEngine("PUT"):
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
    await create_default_asset(
        dataset, version, input_data, None, callback,
    )

    # Get the logs in case something went wrong
    _print_logs(logs)

    # If everything worked, version should be set to "saved"
    row = await versions.get_version(dataset, version)
    assert row.status == "saved"

    # There should be a table called "test"."v1.1.1" with one row
    async with ContextEngine("GET"):
        count = await db.scalar(db.text('SELECT count(*) FROM test."v1.1.1"'))
    assert count == 1

    # The geometry should also be accessible via geostore
    async with ContextEngine("GET"):
        rows: List[Geostore] = await Geostore.query.gino.all()

    assert len(rows) == 1
    assert rows[0].gfw_geostore_id == UUID("b9faa657-34c9-96d4-fce4-8bb8a1507cb3")


@pytest.mark.asyncio
async def test_table_source_asset(batch_client):
    # TODO: define what a callback should do
    async def callback(message):
        pass

    _, logs = batch_client

    # Upload file to mocked S3 bucket
    s3_client = boto3.client(
        "s3", region_name=AWS_REGION, endpoint_url="http://motoserver:5000"
    )

    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.upload_file(TSV_PATH, BUCKET, TSV_NAME)

    dataset = "table_test"
    version = "v202002.1"

    # define partition schema
    partition_schema = dict()
    years = range(2011, 2022)
    for year in years:
        for week in range(1, 54):
            try:
                week = f"y{year}_w{week:02}"
                start = pendulum.parse(f"{year}-W{week}").to_date_string()
                end = pendulum.parse(f"{year}-W{week}").add(days=7).to_date_string()
                partition_schema[week] = (start, end)

            except ParserError:
                # Year has only 52 weeks
                pass

    input_data = {
        "source_type": "table",
        "source_uri": [f"s3://{BUCKET}/{TSV_NAME}"],
        "creation_options": {
            "src_driver": "TSV",
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
    async with ContextEngine("PUT"):
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
    await create_default_asset(
        dataset, version, input_data, None, callback,
    )

    # Get the logs in case something went wrong
    _print_logs(logs)

    # If everything worked, version should be set to "saved"
    row = await versions.get_version(dataset, version)
    assert row.status == "saved"

    # There should be a table called "test"."v1.1.1" with one row
    async with ContextEngine("GET"):
        count = await db.scalar(
            db.text(f'SELECT count(*) FROM "{dataset}"."{version}"')
        )
        partition_count = await db.scalar(
            db.text(
                f"""SELECT count(i.inhrelid::regclass) FROM pg_inherits i WHERE  i.inhparent = '"{dataset}"."{version}"'::regclass"""
            )
        )
        index_count = await db.scalar(
            db.text(
                f" SELECT count(indexname) FROM pg_indexes WHERE schemaname = '{dataset}' AND tablename = '{version}';"
            )
        )

    assert count == 99
    assert partition_count == len(partition_schema)
    assert index_count == len(input_data["creation_options"]["indices"])


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
