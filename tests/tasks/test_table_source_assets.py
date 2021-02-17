import json

import httpx
import pendulum
import pytest
from pendulum.parsing.exceptions import ParserError

from app.application import ContextEngine, db
from app.utils.aws import get_s3_client

from .. import APPEND_TSV_NAME, BUCKET, PORT, TSV_NAME, TSV_PATH
from ..utils import create_default_asset, poll_jobs
from . import check_asset_status, check_task_status, check_version_status


@pytest.mark.asyncio
async def test_table_source_asset(batch_client, async_client):
    _, logs = batch_client

    ############################
    # Setup test
    ############################

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
        "creation_options": {
            "source_type": "table",
            "source_uri": [f"s3://{BUCKET}/{TSV_NAME}"],
            "source_driver": "text",
            "delimiter": "\t",
            "has_header": True,
            "latitude": "latitude",
            "longitude": "longitude",
            "cluster": {
                "index_type": "btree",
                "column_names": ["iso", "adm1", "adm2", "alert__date"],
            },
            "partitions": {
                "partition_type": "range",
                "partition_column": "alert__date",
                "partition_schema": partition_schema,
            },
            "indices": [
                {"index_type": "gist", "column_names": ["geom"]},
                {"index_type": "gist", "column_names": ["geom_wm"]},
                {
                    "index_type": "btree",
                    "column_names": ["iso", "adm1", "adm2", "alert__date"],
                },
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

    #####################
    # Test asset creation
    #####################

    asset = await create_default_asset(
        dataset,
        version,
        version_payload=input_data,
        execute_batch_jobs=True,
        logs=logs,
        async_client=async_client,
    )
    asset_id = asset["asset_id"]

    await check_version_status(dataset, version, 3)
    await check_asset_status(dataset, version, 1)
    await check_task_status(asset_id, 14, "cluster_partitions_3")

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

    ########################
    # Append
    #########################

    httpx.delete(f"http://localhost:{PORT}")

    # Create default asset in mocked BATCH

    httpx.delete(f"http://localhost:{PORT}")

    response = await async_client.post(
        f"/dataset/{dataset}/{version}/append",
        json={"source_uri": [f"s3://{BUCKET}/{APPEND_TSV_NAME}"]},
    )
    assert response.status_code == 200

    response = await async_client.get(f"/dataset/{dataset}/{version}/change_log")
    assert response.status_code == 200
    tasks = json.loads(response.json()["data"][-1]["detail"])
    task_ids = [task["job_id"] for task in tasks]
    # print(task_ids)

    # make sure, all jobs completed
    status = await poll_jobs(task_ids, logs=logs, async_client=async_client)

    assert status == "saved"

    await check_version_status(dataset, version, 6)
    await check_asset_status(dataset, version, 2)

    # The table should now have 101 rows after append
    async with ContextEngine("READ"):
        count = await db.scalar(
            db.text(
                f"""
                    SELECT count(*)
                        FROM "{dataset}"."{version}";"""
            )
        )

    assert count == 101

    # The table should now have no empty geometries
    async with ContextEngine("READ"):
        count = await db.scalar(
            db.text(
                f"""
                    SELECT count(*)
                        FROM "{dataset}"."{version}" WHERE geom IS NULL;"""
            )
        )

    assert count == 0


# @pytest.mark.skip(
#     reason="Something weird going on with how I'm creating virtual CSVs. Fix later."
# )
@pytest.mark.asyncio
async def test_table_source_asset_parallel(batch_client, async_client):
    _, logs = batch_client

    ############################
    # Setup test
    ############################

    dataset = "table_test"
    version = "v202002.1"

    s3_client = get_s3_client()

    for i in range(2, 101):
        s3_client.upload_file(TSV_PATH, BUCKET, f"test_{i}.tsv")

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
        "creation_options": {
            "source_type": "table",
            "source_uri": [f"s3://{BUCKET}/{TSV_NAME}"]
            + [f"s3://{BUCKET}/test_{i}.tsv" for i in range(2, 101)],
            "source_driver": "text",
            "delimiter": "\t",
            "has_header": True,
            "latitude": "latitude",
            "longitude": "longitude",
            "cluster": {"index_type": "gist", "column_names": ["geom_wm"]},
            "partitions": {
                "partition_type": "range",
                "partition_column": "alert__date",
                "partition_schema": partition_schema,
            },
            "indices": [
                {"index_type": "gist", "column_names": ["geom"]},
                {"index_type": "gist", "column_names": ["geom_wm"]},
                {"index_type": "btree", "column_names": ["alert__date"]},
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

    #####################
    # Test asset creation
    #####################

    asset = await create_default_asset(
        dataset,
        version,
        version_payload=input_data,
        execute_batch_jobs=True,
        logs=logs,
        async_client=async_client,
    )
    asset_id = asset["asset_id"]

    await check_version_status(dataset, version, 3)
    await check_asset_status(dataset, version, 1)
    await check_task_status(asset_id, 26, "cluster_partitions_3")

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

    assert count == 9900
    assert partition_count == len(partition_schema)
    assert index_count == partition_count * len(
        input_data["creation_options"]["indices"]
    )
    assert cluster_count == len(partition_schema)
