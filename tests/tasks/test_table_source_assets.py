import copy
import json
import string
from typing import Dict

import httpx
import pendulum
import pytest
from httpx import AsyncClient
from pendulum.parsing.exceptions import ParserError

from app.application import ContextEngine, db

from .. import APPEND_TSV_NAME, BUCKET, PORT, TSV_NAME
from ..utils import (
    create_dataset,
    create_default_asset,
    get_cluster_count,
    get_index_count,
    get_partition_count,
    get_row_count,
    poll_jobs,
)
from . import check_asset_status, check_task_status, check_version_status

basic_table_input_data: Dict = {
    "creation_options": {
        "source_type": "table",
        "source_uri": [f"s3://{BUCKET}/{TSV_NAME}"],
        "source_driver": "text",
        "delimiter": "\t",
        "has_header": True,
        "timeout": 600,
        "latitude": None,
        "longitude": None,
        "create_dynamic_vector_tile_cache": False,
        "partitions": None,
        "cluster": None,
        "indices": [],
        "constraints": None,
        "table_schema": [
            {
                "name": "rspo_oil_palm__certification_status",
                "data_type": "text",
            },
            {"name": "per_forest_concession__type", "data_type": "text"},
            {"name": "idn_forest_area__type", "data_type": "text"},
            {"name": "alert__count", "data_type": "integer"},
            {"name": "adm1", "data_type": "integer"},
            {"name": "adm2", "data_type": "integer"},
        ],
    }
}


@pytest.mark.asyncio
async def test_prove_correct_schema():
    from app.models.pydantic.creation_options import TableSourceCreationOptions

    input_data: Dict = copy.deepcopy(basic_table_input_data["creation_options"])
    input_data["cluster"] = {
        "index_type": "btree",
        "column_names": ["iso", "adm1", "adm2", "alert__date"],
    }
    bar = TableSourceCreationOptions(**input_data)
    assert bar.cluster is not None


@pytest.mark.asyncio
async def test_table_source_asset_minimal(batch_client, async_client: AsyncClient):
    _, logs = batch_client

    ############################
    # Setup test
    ############################
    dataset = "table_test"
    version = "v202002.1"
    input_data: Dict = copy.deepcopy(basic_table_input_data)

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

    #################
    # Check results
    #################
    await check_version_status(dataset, version, 2)
    await check_asset_status(dataset, version, 1)  # There should be 1 asset

    # There should be the following tasks:
    # 1 to create the table schema
    # 0 to partition because we didn't specify it
    # 1 to load the data
    # 0 to add point geometry because we didn't specify it
    # 0 to add indices because we didn't specify them
    # 0 to add clustering because we didn't specify it
    await check_task_status(asset_id, 2, "load_tabular_data_0")

    # There should be a table called "table_test"."v202002.1" with 99 rows.
    # It should have the right amount of partitions and indices
    async with ContextEngine("READ"):
        row_count = await get_row_count(db, dataset, version)
        partition_count = await get_partition_count(db, dataset, version)
        index_count = await get_index_count(db, dataset, version)
        cluster_count = await get_cluster_count(db)

    assert row_count == 99
    assert partition_count == 0
    assert index_count == 0
    assert cluster_count == 0


@pytest.mark.asyncio
async def test_table_source_asset_indices(batch_client, async_client: AsyncClient):
    _, logs = batch_client

    ############################
    # Setup test
    ############################
    dataset = "table_test"
    version = "v202002.1"
    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["indices"] = [
        {"index_type": "btree", "column_names": ["iso"]},
        {"index_type": "hash", "column_names": ["rspo_oil_palm__certification_status"]},
        {"index_type": "btree", "column_names": ["iso", "adm1", "adm2", "alert__date"]},
    ]

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

    #################
    # Check results
    #################
    await check_version_status(dataset, version, 2)
    await check_asset_status(dataset, version, 1)  # There should be 1 asset

    # There should be the following tasks:
    # 1 to create the table schema
    # 0 to partition because we didn't specify it
    # 1 to load the data
    # 0 to add point geometry because we didn't specify it
    # 3 to add indices
    # 0 to add clustering because we didn't specify it
    await check_task_status(asset_id, 5, "create_index_iso_adm1_adm2_alert__date_btree")

    # There should be a table called "table_test"."v202002.1" with 99 rows.
    # It should have the right amount of partitions and indices
    async with ContextEngine("READ"):
        row_count = await get_row_count(db, dataset, version)
        partition_count = await get_partition_count(db, dataset, version)
        index_count = await get_index_count(db, dataset, version)
        cluster_count = await get_cluster_count(db)

    assert row_count == 99
    assert partition_count == 0
    # postgres12 also adds indices to the main table, hence there are more indices than partitions
    assert index_count == (partition_count + 1) * len(
        input_data["creation_options"]["indices"]
    )
    assert cluster_count == 0


@pytest.mark.asyncio
async def test_table_source_asset_lat_long(batch_client, async_client: AsyncClient):
    _, logs = batch_client

    ############################
    # Setup test
    ############################
    dataset = "table_test"
    version = "v202002.1"
    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["latitude"] = "latitude"
    input_data["creation_options"]["longitude"] = "longitude"
    input_data["creation_options"]["create_dynamic_vector_tile_cache"] = True

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

    #################
    # Check results
    #################
    await check_version_status(dataset, version, 3)
    # There should be an extra asset from the dynamic vector tile cache
    await check_asset_status(dataset, version, 2)

    # There should be the following tasks:
    # 1 to create the table schema
    # 0 to partition because we didn't specify it
    # 1 to add point geometry
    # 1 to load the data
    # 0 to add indices because we didn't specify it
    # 0 to add clustering because we didn't specify it
    await check_task_status(asset_id, 3, "load_tabular_data_0")

    # There should be a table called "table_test"."v202002.1" with 99 rows.
    # It should have the right amount of partitions and indices
    async with ContextEngine("READ"):
        row_count = await get_row_count(db, dataset, version)
        partition_count = await get_partition_count(db, dataset, version)
        index_count = await get_index_count(db, dataset, version)
        cluster_count = await get_cluster_count(db)

    assert row_count == 99
    assert partition_count == 0
    assert index_count == 0
    assert cluster_count == 0


@pytest.mark.asyncio
async def test_table_source_asset_partition(batch_client, async_client: AsyncClient):
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

    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["partitions"] = {
        "partition_type": "range",
        "partition_column": "alert__date",
        "partition_schema": partition_schema,
    }

    partition_count_expected = len(partition_schema)

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

    #################
    # Check results
    #################
    await check_version_status(dataset, version, 2)
    await check_asset_status(dataset, version, 1)  # There should be 1 asset

    # There should be the following tasks:
    # 1 to create the table schema
    # 4 to partition
    # 1 to load the data
    # 0 to add point geometry
    # 0 to add indices
    # 0 to add clustering because we didn't specify it
    await check_task_status(asset_id, 6, "load_tabular_data_0")

    # There should be a table called "table_test"."v202002.1" with 99 rows.
    # It should have the right amount of partitions and indices
    async with ContextEngine("READ"):
        row_count = await get_row_count(db, dataset, version)
        partition_count = await get_partition_count(db, dataset, version)
        index_count = await get_index_count(db, dataset, version)
        cluster_count = await get_cluster_count(db)

    assert row_count == 99
    assert partition_count == partition_count_expected
    # postgres12 also adds indices to the main table, hence there are more indices than partitions
    assert index_count == 0
    assert cluster_count == 0


@pytest.mark.asyncio
async def test_table_source_asset_cluster(batch_client, async_client: AsyncClient):
    _, logs = batch_client

    ############################
    # Setup test
    ############################
    dataset = "table_test"
    version = "v202002.1"
    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["cluster"] = {
        "index_type": "btree",
        "column_names": ["iso", "adm1", "adm2", "alert__date"],
    }
    input_data["creation_options"]["indices"] = [
        {"index_type": "btree", "column_names": ["iso"]},
        {"index_type": "hash", "column_names": ["rspo_oil_palm__certification_status"]},
        {"index_type": "btree", "column_names": ["iso", "adm1", "adm2", "alert__date"]},
    ]

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

    #################
    # Check results
    #################
    await check_version_status(dataset, version, 2)
    await check_asset_status(dataset, version, 1)  # There should be 1 asset

    # There should be the following tasks:
    # 1 to create the table schema
    # 0 to partition because we didn't specify it
    # 1 to load the data
    # 0 to add point geometry because we didn't specify it
    # 3 to add indices
    # 1 to add clustering
    await check_task_status(asset_id, 6, "cluster_table")

    # There should be a table called "table_test"."v202002.1" with 99 rows.
    # It should have the right amount of partitions and indices
    async with ContextEngine("READ"):
        row_count = await get_row_count(db, dataset, version)
        partition_count = await get_partition_count(db, dataset, version)
        index_count = await get_index_count(db, dataset, version)
        cluster_count = await get_cluster_count(db)

    assert row_count == 99
    assert partition_count == 0
    # postgres12 also adds indices to the main table, hence there are more indices than partitions
    assert index_count == (partition_count + 1) * len(
        input_data["creation_options"]["indices"]
    )
    assert cluster_count == 1


@pytest.mark.asyncio
async def test_table_source_asset_constraints(batch_client, async_client: AsyncClient):
    _, logs = batch_client

    ############################
    # Setup test
    ############################
    dataset = "table_test"
    version = "v202002.1"
    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["constraints"] = [
        {"constraint_type": "unique", "column_names": ["adm1", "adm2", "alert__date"]}
    ]

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

    #################
    # Check results
    #################
    await check_version_status(dataset, version, 2)
    await check_asset_status(dataset, version, 1)  # There should be 1 asset

    # There should be the following tasks:
    # 1 to create the table schema
    # 0 to partition because we didn't specify it
    # 1 to load the data
    # 0 to add point geometry because we didn't specify it
    # 0 to add indices
    # 0 to add clustering because we didn't specify it
    await check_task_status(asset_id, 2, "load_tabular_data_0")

    # There should be a table called "table_test"."v202002.1" with 99 rows.
    # It should have the right amount of partitions and indices
    async with ContextEngine("READ"):
        row_count = await get_row_count(db, dataset, version)
        partition_count = await get_partition_count(db, dataset, version)
        index_count = await get_index_count(db, dataset, version)
        cluster_count = await get_cluster_count(db)

    assert row_count == 99
    assert partition_count == 0
    # postgres12 also adds indices to the main table, hence there are more indices than partitions
    assert index_count == len(input_data["creation_options"]["constraints"])
    assert cluster_count == 0


@pytest.mark.asyncio
async def test_table_source_asset_everything(batch_client, async_client: AsyncClient):
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

    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["timeout"] = 600
    input_data["creation_options"]["latitude"] = "latitude"
    input_data["creation_options"]["longitude"] = "longitude"
    input_data["creation_options"]["create_dynamic_vector_tile_cache"] = True
    input_data["creation_options"]["partitions"] = {
        "partition_type": "range",
        "partition_column": "alert__date",
        "partition_schema": partition_schema,
    }
    input_data["creation_options"]["constraints"] = [
        {"constraint_type": "unique", "column_names": ["adm1", "adm2", "alert__date"]}
    ]
    input_data["creation_options"]["indices"] = [
        {"index_type": "btree", "column_names": ["iso"]},
        {"index_type": "hash", "column_names": ["rspo_oil_palm__certification_status"]},
        {"index_type": "btree", "column_names": ["iso", "adm1", "adm2", "alert__date"]},
    ]
    input_data["creation_options"]["cluster"] = {
        "index_type": "btree",
        "column_names": ["iso", "adm1", "adm2", "alert__date"],
    }

    partition_count_expected = len(partition_schema)

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

    #################
    # Check results
    #################
    await check_version_status(dataset, version, 3)
    # There should be an extra asset from the dynamic vector tile cache
    await check_asset_status(dataset, version, 2)

    # There should be the following tasks:
    # 1 to create the table schema
    # 4 to partition
    # 1 to load the data
    # 1 to add point geometry
    # 3 to add indices
    # 4 to add clustering
    await check_task_status(asset_id, 14, "cluster_partitions_3")

    # There should be a table called "table_test"."v202002.1" with 99 rows.
    # It should have the right amount of partitions and indices
    async with ContextEngine("READ"):
        row_count = await get_row_count(db, dataset, version)
        partition_count = await get_partition_count(db, dataset, version)
        index_count = await get_index_count(db, dataset, version)
        cluster_count = await get_cluster_count(db)

    assert row_count == 99
    assert partition_count == partition_count_expected
    # Disclaimer: These next two values are observed... I have no idea how
    # to calculate whether or not they are correct. Probably by some
    # multiplication of indices, partitions, constraints, and clusters
    assert index_count == 632
    assert cluster_count == 157


@pytest.mark.asyncio
async def test_table_source_asset_append(batch_client, async_client: AsyncClient):
    _, logs = batch_client

    ############################
    # Setup test
    ############################
    dataset = "table_test"
    version = "v202002.1"
    input_data: Dict = copy.deepcopy(basic_table_input_data)

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

    #################
    # Check results
    #################
    await check_version_status(dataset, version, 2)
    await check_asset_status(dataset, version, 1)  # There should be 1 asset

    # There should be the following tasks:
    # 1 to create the table schema
    # 0 to partition because we didn't specify it
    # 1 to load the data
    # 0 to add point geometry because we didn't specify it
    # 0 to add indices because we didn't specify them
    # 0 to add clustering because we didn't specify it
    await check_task_status(asset_id, 2, "load_tabular_data_0")

    ########################
    # Append
    #########################
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

    # make sure all jobs completed
    status = await poll_jobs(task_ids, logs=logs, async_client=async_client)
    assert status == "saved"

    await check_version_status(dataset, version, 4)
    await check_asset_status(dataset, version, 1)


@pytest.mark.asyncio
async def test_table_source_asset_append_with_geom(
    batch_client, async_client: AsyncClient
):
    _, logs = batch_client

    ############################
    # Setup test
    ############################
    dataset = "table_test"
    version = "v202002.1"
    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["latitude"] = "latitude"
    input_data["creation_options"]["longitude"] = "longitude"

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

    #################
    # Check results
    #################
    await check_version_status(dataset, version, 2)
    await check_asset_status(dataset, version, 1)  # There should be 1 asset

    # There should be the following tasks:
    # 1 to create the table schema
    # 0 to partition because we didn't specify it
    # 1 to load the data
    # 1 to add point geometry
    # 0 to add indices because we didn't specify them
    # 0 to add clustering because we didn't specify it
    await check_task_status(asset_id, 3, "load_tabular_data_0")

    ########################
    # Append
    #########################
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

    # make sure all jobs completed
    status = await poll_jobs(task_ids, logs=logs, async_client=async_client)
    assert status == "saved"

    await check_version_status(dataset, version, 4)
    await check_asset_status(dataset, version, 1)


@pytest.mark.asyncio
async def test_table_source_asset_too_many_columns_in_unique_constraint(
    async_client: AsyncClient,
):
    dataset = "table_test"
    version = "v202002.1"
    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["constraints"] = [
        {
            "constraint_type": "unique",
            "column_names": [f"a_{letter}" for letter in string.ascii_letters],
        }
    ]

    await create_dataset(dataset, async_client)

    resp = await async_client.put(f"/dataset/{dataset}/{version}", json=input_data)

    assert resp.status_code == 422
    assert "ensure this value has at most 32 items" in resp.text


@pytest.mark.asyncio
async def test_table_source_asset_no_columns_in_unique_constraint(
    async_client: AsyncClient,
):
    dataset = "table_test"
    version = "v202002.1"
    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["constraints"] = [
        {"constraint_type": "unique", "column_names": []}
    ]

    await create_dataset(dataset, async_client)

    resp = await async_client.put(f"/dataset/{dataset}/{version}", json=input_data)

    assert resp.status_code == 422
    assert "ensure this value has at least 1 items" in resp.text


@pytest.mark.asyncio
async def test_table_source_asset_too_many_unique_constraints(
    async_client: AsyncClient,
):
    dataset = "table_test"
    version = "v202002.1"
    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["constraints"] = [
        {"constraint_type": "unique", "column_names": ["foo", "bar"]},
        {
            "constraint_type": "unique",
            "column_names": [f"a_{letter}" for letter in string.ascii_lowercase],
        },
    ]

    await create_dataset(dataset, async_client)

    resp = await async_client.put(f"/dataset/{dataset}/{version}", json=input_data)

    assert resp.status_code == 422
    assert "Currently cannot specify more than 1 unique constraint" in resp.text


@pytest.mark.asyncio
async def test_table_source_asset_too_many_columns_in_index(async_client: AsyncClient):
    dataset = "table_test"
    version = "v202002.1"
    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["indices"] = [
        {
            "index_type": "btree",
            "column_names": [f"a_{letter}" for letter in string.ascii_letters],
        }
    ]

    await create_dataset(dataset, async_client)

    resp = await async_client.put(f"/dataset/{dataset}/{version}", json=input_data)

    assert resp.status_code == 422
    assert "ensure this value has at most 32 items" in resp.text


@pytest.mark.asyncio
async def test_table_source_asset_no_columns_in_index(async_client: AsyncClient):
    dataset = "table_test"
    version = "v202002.1"
    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["indices"] = [
        {"index_type": "btree", "column_names": []}
    ]

    await create_dataset(dataset, async_client)

    resp = await async_client.put(f"/dataset/{dataset}/{version}", json=input_data)

    assert resp.status_code == 422
    assert "ensure this value has at least 1 items" in resp.text
