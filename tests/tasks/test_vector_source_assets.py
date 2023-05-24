import json
from typing import List
from unittest.mock import patch
from uuid import UUID

import httpx
import pytest
from httpx import AsyncClient

from app.application import ContextEngine, db
from app.crud.tasks import get_tasks
from app.models.orm.geostore import Geostore
from app.models.orm.tasks import Task as ORMTask
from app.models.pydantic.geostore import Geometry, GeostoreCommon

from .. import (
    BUCKET,
    CSV2_NAME,
    CSV_NAME,
    GEOJSON_NAME,
    GEOJSON_NAME2,
    GEOJSON_PATH,
    GEOJSON_PATH2,
    PORT,
    SHP_NAME,
)
from ..utils import create_default_asset, poll_jobs, version_metadata
from . import (
    check_asset_status,
    check_dynamic_vector_tile_cache_status,
    check_task_status,
    check_version_status,
)


@pytest.mark.asyncio
async def test_vector_source_asset(batch_client, async_client: AsyncClient):
    _, logs = batch_client

    ############################
    # Setup test
    ############################

    dataset = "test"
    sources = (SHP_NAME, GEOJSON_NAME)

    for i, source in enumerate(sources):
        version = f"v1.1.{i}"
        input_data = {
            "creation_options": {
                "source_type": "vector",
                "source_uri": [f"s3://{BUCKET}/{source}"],
                "source_driver": "GeoJSON",  # FIXME: True for ESRI Shapefile?
                "create_dynamic_vector_tile_cache": True,
            },
            "metadata": version_metadata,
        }

        # we only need to create the dataset once
        if i > 0:
            skip_dataset = True
        else:
            skip_dataset = False
        asset = await create_default_asset(
            dataset,
            version,
            version_payload=input_data,
            async_client=async_client,
            logs=logs,
            execute_batch_jobs=True,
            skip_dataset=skip_dataset,
        )
        asset_id = asset["asset_id"]

        await check_version_status(dataset, version, 3)
        await check_asset_status(dataset, version, 2)
        await check_task_status(asset_id, 8, "inherit_from_geostore")

        # There should be a table called "test"."v1.1.1" with one row
        async with ContextEngine("READ"):
            count = await db.scalar(
                db.text(f'SELECT count(*) FROM {dataset}."{version}"')
            )
        assert count == 1

        # The geometry should also be accessible via geostore
        async with ContextEngine("READ"):
            rows: List[Geostore] = await Geostore.query.gino.all()

        assert len(rows) == 1 + i
        assert rows[0].gfw_geostore_id == UUID("1b368160-caf8-2bd7-819a-ad4949361f02")

        await check_dynamic_vector_tile_cache_status(dataset, version)

        # Queries

        response = await async_client.get(
            f"/dataset/{dataset}/{version}/query?sql=select count(*) from mytable;",
            follow_redirects=True,
        )
        assert response.status_code == 200
        assert len(response.json()["data"]) == 1
        assert response.json()["data"][0]["count"] == 1

        with open(GEOJSON_PATH, "r") as geojson:
            raw_geom = json.load(geojson)["features"][0]["geometry"]
            geom = Geometry(type=raw_geom["type"], coordinates=raw_geom["coordinates"])
            geostore = GeostoreCommon(
                geojson=geom,
                geostore_id="17076d5ea9f214a5bdb68cc40433addb",
                area__ha=214324,
                bbox=[0, 0, 10, 10],
            )
            with patch(
                "app.utils.rw_api.get_geostore",
                return_value=geostore,
            ):
                response = await async_client.get(
                    f"/dataset/{dataset}/{version}/query?sql=SELECT count(*) FROM mytable&geostore_id=17076d5ea9f214a5bdb68cc40433addb&geostore_origin=rw",
                    follow_redirects=True,
                )
        assert response.status_code == 200
        assert len(response.json()["data"]) == 1
        assert response.json()["data"][0]["count"] == 1

        with open(GEOJSON_PATH2, "r") as geojson:
            raw_geom = json.load(geojson)["features"][0]["geometry"]
            geom = Geometry(type=raw_geom["type"], coordinates=raw_geom["coordinates"])
            geostore = GeostoreCommon(
                geojson=geom,
                geostore_id="17076d5ea9f214a5bdb68cc40433addb",
                area__ha=214324,
                bbox=[0, 0, 10, 10],
            )
            with patch(
                "app.utils.rw_api.get_geostore",
                return_value=geostore,
            ):
                response = await async_client.get(
                    f"/dataset/{dataset}/{version}/query?sql=SELECT count(*) FROM mytable&geostore_id=17076d5ea9f214a5bdb68cc40433addb&geostore_origin=rw",
                    follow_redirects=True,
                )
        assert response.status_code == 200
        assert len(response.json()["data"]) == 1
        assert response.json()["data"][0]["count"] == 0

        # Stats
        # TODO: We currently don't compute stats, will need update this test once feature is available

        response = await async_client.get(f"/dataset/{dataset}/{version}/stats")
        assert response.status_code == 200
        assert response.json()["data"] is None

        # Fields
        response = await async_client.get(f"/dataset/{dataset}/{version}/fields")
        assert response.status_code == 200
        if i == 0:
            assert response.json()["data"] == [
                {
                    "name": "gfw_fid",
                    "alias": "gfw_fid",
                    "description": None,
                    "data_type": "integer",
                    "is_feature_info": True,
                    "is_filter": True,
                    "unit": None,
                },
                {
                    "name": "fid",
                    "alias": "fid",
                    "description": None,
                    "data_type": "numeric",
                    "is_feature_info": True,
                    "is_filter": True,
                    "unit": None,
                },
                {
                    "name": "geom",
                    "alias": "geom",
                    "description": None,
                    "data_type": "geometry",
                    "is_feature_info": False,
                    "is_filter": False,
                    "unit": None,
                },
                {
                    "name": "geom_wm",
                    "alias": "geom_wm",
                    "description": None,
                    "data_type": "geometry",
                    "is_feature_info": False,
                    "is_filter": False,
                    "unit": None,
                },
                {
                    "name": "gfw_area__ha",
                    "alias": "gfw_area__ha",
                    "description": None,
                    "data_type": "numeric",
                    "is_feature_info": True,
                    "is_filter": True,
                    "unit": None,
                },
                {
                    "name": "gfw_geostore_id",
                    "alias": "gfw_geostore_id",
                    "description": None,
                    "data_type": "uuid",
                    "is_feature_info": True,
                    "is_filter": True,
                    "unit": None,
                },
                {
                    "name": "gfw_geojson",
                    "alias": "gfw_geojson",
                    "description": None,
                    "data_type": "text",
                    "is_feature_info": False,
                    "is_filter": False,
                    "unit": None,
                },
                {
                    "name": "gfw_bbox",
                    "alias": "gfw_bbox",
                    "description": None,
                    "data_type": "ARRAY",
                    "is_feature_info": False,
                    "is_filter": False,
                    "unit": None,
                },
                {
                    "name": "created_on",
                    "alias": "created_on",
                    "description": None,
                    "data_type": "timestamp without time zone",
                    "is_feature_info": False,
                    "is_filter": False,
                    "unit": None,
                },
                {
                    "name": "updated_on",
                    "alias": "updated_on",
                    "description": None,
                    "data_type": "timestamp without time zone",
                    "is_feature_info": False,
                    "is_filter": False,
                    "unit": None,
                },
            ]
        else:
            # JSON file does not have fid field
            assert response.json()["data"] == [
                {
                    "name": "gfw_fid",
                    "alias": "gfw_fid",
                    "description": None,
                    "data_type": "integer",
                    "is_feature_info": True,
                    "is_filter": True,
                    "unit": None,
                },
                {
                    "name": "geom",
                    "alias": "geom",
                    "description": None,
                    "data_type": "geometry",
                    "is_feature_info": False,
                    "is_filter": False,
                    "unit": None,
                },
                {
                    "name": "geom_wm",
                    "alias": "geom_wm",
                    "description": None,
                    "data_type": "geometry",
                    "is_feature_info": False,
                    "is_filter": False,
                    "unit": None,
                },
                {
                    "name": "gfw_area__ha",
                    "alias": "gfw_area__ha",
                    "description": None,
                    "data_type": "numeric",
                    "is_feature_info": True,
                    "is_filter": True,
                    "unit": None,
                },
                {
                    "name": "gfw_geostore_id",
                    "alias": "gfw_geostore_id",
                    "description": None,
                    "data_type": "uuid",
                    "is_feature_info": True,
                    "is_filter": True,
                    "unit": None,
                },
                {
                    "name": "gfw_geojson",
                    "alias": "gfw_geojson",
                    "description": None,
                    "data_type": "text",
                    "is_feature_info": False,
                    "is_filter": False,
                    "unit": None,
                },
                {
                    "name": "gfw_bbox",
                    "alias": "gfw_bbox",
                    "description": None,
                    "data_type": "ARRAY",
                    "is_feature_info": False,
                    "is_filter": False,
                    "unit": None,
                },
                {
                    "name": "created_on",
                    "alias": "created_on",
                    "description": None,
                    "data_type": "timestamp without time zone",
                    "is_feature_info": False,
                    "is_filter": False,
                    "unit": None,
                },
                {
                    "name": "updated_on",
                    "alias": "updated_on",
                    "description": None,
                    "data_type": "timestamp without time zone",
                    "is_feature_info": False,
                    "is_filter": False,
                    "unit": None,
                },
            ]

        httpx.delete(f"http://localhost:{PORT}")

    response = await async_client.get(f"/asset/{asset_id}")
    assert response.status_code == 200

    response = await async_client.get("/dataset/different/v1.1.1/assets")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_vector_source_asset_csv(batch_client, async_client: AsyncClient):
    _, logs = batch_client

    dataset = "test"
    version = "v1.1.1"
    input_data = {
        "creation_options": {
            "source_type": "vector",
            "source_uri": [f"s3://{BUCKET}/{CSV_NAME}"],
            "source_driver": "CSV",
            "table_schema": [{"name": "alert__date", "data_type": "date"}],
            "create_dynamic_vector_tile_cache": True,
        },
    }

    asset = await create_default_asset(
        dataset,
        version,
        version_payload=input_data,
        async_client=async_client,
        logs=logs,
        execute_batch_jobs=True,
    )
    asset_id = asset["asset_id"]

    await check_version_status(dataset, version, 3)
    await check_asset_status(dataset, version, 2)
    await check_task_status(asset_id, 8, "inherit_from_geostore")

    # There should be a table called "test"."v1.1.1" with one row
    async with ContextEngine("READ"):
        count = await db.scalar(db.text(f'SELECT count(*) FROM {dataset}."{version}"'))
    assert count == 1


@pytest.mark.asyncio
async def test_vector_source_asset_csv_append(batch_client, async_client: AsyncClient):
    _, logs = batch_client

    dataset = "test"
    version = "v1.1.1"
    input_data = {
        "creation_options": {
            "source_type": "vector",
            "source_uri": [f"s3://{BUCKET}/{CSV_NAME}"],
            "source_driver": "CSV",
            "table_schema": [{"name": "alert__date", "data_type": "date"}],
            "create_dynamic_vector_tile_cache": False,
            "add_to_geostore": False,
            "indices": [],
        },
    }

    asset = await create_default_asset(
        dataset,
        version,
        version_payload=input_data,
        async_client=async_client,
        logs=logs,
        execute_batch_jobs=True,
    )
    asset_id = asset["asset_id"]

    # There should be a table called "test"."v1.1.1" with one row
    async with ContextEngine("READ"):
        count = await db.scalar(db.text(f'SELECT count(*) FROM {dataset}."{version}"'))
    assert count == 1

    # Now test appending
    resp = await async_client.post(
        f"/dataset/{dataset}/{version}/append",
        json={"source_uri": [f"s3://{BUCKET}/{CSV2_NAME}"]},
    )
    assert resp.status_code == 200

    tasks: List[ORMTask] = await get_tasks(asset_id)
    task_ids = [str(task.task_id) for task in tasks]
    status = await poll_jobs(task_ids, logs=logs, async_client=async_client)
    assert status == "saved"

    # Now "test"."v1.1.1" should have an additional row
    async with ContextEngine("READ"):
        count = await db.scalar(db.text(f'SELECT count(*) FROM {dataset}."{version}"'))
    assert count == 2


@pytest.mark.asyncio
async def test_vector_source_asset_geojson_append(
    batch_client, async_client: AsyncClient
):
    _, logs = batch_client

    dataset = "test"
    version = "v1.1.2"
    input_data = {
        "creation_options": {
            "source_type": "vector",
            "source_uri": [f"s3://{BUCKET}/{GEOJSON_NAME}"],
            "source_driver": "GeoJSON",
            "create_dynamic_vector_tile_cache": False,
            "add_to_geostore": False,
            "indices": [],
        },
    }

    asset = await create_default_asset(
        dataset,
        version,
        version_payload=input_data,
        async_client=async_client,
        logs=logs,
        execute_batch_jobs=True,
    )
    asset_id = asset["asset_id"]

    # There should be a table called "test"."v1.1.1" with one row
    async with ContextEngine("READ"):
        count = await db.scalar(db.text(f'SELECT count(*) FROM {dataset}."{version}"'))
    assert count == 1

    # Now test appending
    resp = await async_client.post(
        f"/dataset/{dataset}/{version}/append",
        json={"source_uri": [f"s3://{BUCKET}/{GEOJSON_NAME2}"]},
    )
    assert resp.status_code == 200, resp.text

    tasks: List[ORMTask] = await get_tasks(asset_id)
    task_ids = [str(task.task_id) for task in tasks]
    status = await poll_jobs(task_ids, logs=logs, async_client=async_client)
    assert status == "saved"

    # Now "test"."v1.1.1" should have an additional row
    async with ContextEngine("READ"):
        count = await db.scalar(db.text(f'SELECT count(*) FROM {dataset}."{version}"'))
    assert count == 2