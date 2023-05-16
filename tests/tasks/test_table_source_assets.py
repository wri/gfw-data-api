import copy
from typing import Dict

import pendulum
import pytest
from httpx import AsyncClient
from pendulum.parsing.exceptions import ParserError

from app.application import ContextEngine, db

from .. import BUCKET, TSV_NAME
from ..utils import (
    create_default_asset,
    get_cluster_count,
    get_index_count,
    get_partition_count,
    get_row_count,
)
from . import check_asset_status, check_task_status, check_version_status

basic_table_input_data: Dict = {
    "creation_options": {
        "source_type": "table",
        "source_uri": [f"s3://{BUCKET}/{TSV_NAME}"],
        "source_driver": "text",
        "delimiter": "\t",
        "has_header": True,
        "latitude": "latitude",
        "longitude": "longitude",
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
async def test_table_source_asset_basic(batch_client, async_client: AsyncClient):
    _, logs = batch_client

    ############################
    # Setup test
    ############################

    dataset = "table_test"
    version = "v202002.1"

    #####################
    # Test asset creation
    #####################

    input_data: Dict = copy.deepcopy(basic_table_input_data)

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
    await check_asset_status(dataset, version, 1)  # There should be 1 asset
    # There should be 6 tasks:
    # 1 to create the table schema
    # 0 to partition because we didn't specify it
    # 1 to load the data
    # 1 to add point geometry
    # 3 to add indices
    # 0 to add clustering because we didn't specify it
    await check_task_status(asset_id, 6, "create_index_iso_adm1_adm2_alert__date_btree")

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

    partition_count_expected = len(partition_schema)

    #####################
    # Test asset creation
    #####################

    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["partitions"] = {
        "partition_type": "range",
        "partition_column": "alert__date",
        "partition_schema": partition_schema,
    }

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
    await check_asset_status(dataset, version, 1)  # There should be 1 asset

    # There should be the following tasks:
    # 1 to create the table schema
    # 4 to partition
    # 1 to load the data
    # 1 to add point geometry
    # 3 to add indices
    # 0 to add clustering because we didn't specify it
    await check_task_status(
        asset_id, 10, "create_index_iso_adm1_adm2_alert__date_btree"
    )

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
    assert index_count == (partition_count + 1) * len(
        input_data["creation_options"]["indices"]
    )
    assert cluster_count == 0


@pytest.mark.skip("Covers a currently broken corner-case: See GTC-2407")
@pytest.mark.asyncio
async def test_table_source_asset_cluster(batch_client, async_client: AsyncClient):
    _, logs = batch_client

    ############################
    # Setup test
    ############################

    dataset = "table_test"
    version = "v202002.1"

    #####################
    # Test asset creation
    #####################

    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["cluster"] = {
        "index_type": "btree",
        "column_names": ["iso", "adm1", "adm2", "alert__date"],
    }

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
    await check_asset_status(dataset, version, 1)  # There should be 1 asset

    # There should be the following tasks:
    # 1 to create the table schema
    # 0 to partition because we didn't specify it
    # 1 to load the data
    # 1 to add point geometry
    # 3 to add indices
    # 1 to add clustering
    await check_task_status(asset_id, 8, "create_index_iso_adm1_adm2_alert__date_btree")

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

    #####################
    # Test asset creation
    #####################

    input_data: Dict = copy.deepcopy(basic_table_input_data)
    input_data["creation_options"]["constraints"] = [
        {"constraint_type": "unique", "column_names": ["adm1", "adm2", "alert__date"]}
    ]

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
    await check_asset_status(dataset, version, 1)  # There should be 1 asset

    # There should be the following tasks:
    # 1 to create the table schema
    # 0 to partition because we didn't specify it
    # 1 to load the data
    # 1 to add point geometry
    # 3 to add indices
    # 0 to add clustering because we didn't specify it
    await check_task_status(asset_id, 6, "create_index_iso_adm1_adm2_alert__date_btree")

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
    ) + len(input_data["creation_options"]["constraints"])
    assert cluster_count == 0
