import json
from typing import List
from uuid import UUID

import pytest
import requests
from mock import patch

from app.application import ContextEngine, db
from app.models.orm.geostore import Geostore

from .. import BUCKET, GEOJSON_NAME, GEOJSON_PATH, GEOJSON_PATH2, PORT, SHP_NAME
from ..utils import create_default_asset
from . import (
    check_asset_status,
    check_dynamic_vector_tile_cache_status,
    check_task_status,
    check_version_status,
)


@pytest.mark.asyncio
async def test_vector_source_asset(batch_client, async_client):
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
                "source_driver": "GeoJSON",
                "create_dynamic_vector_tile_cache": True,
            },
            "metadata": {},
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
        await check_asset_status(dataset, version, 1)
        await check_task_status(asset_id, 7, "inherit_from_geostore")

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
        assert rows[0].gfw_geostore_id == UUID("23866dd0-9b1a-d742-a7e3-21dd255481dd")

        await check_dynamic_vector_tile_cache_status(dataset, version)

        # Queries

        response = await async_client.get(
            f"/dataset/{dataset}/{version}/query?sql=select count(*) from mytable;"
        )
        assert response.status_code == 200
        assert len(response.json()["data"]) == 1
        assert response.json()["data"][0]["count"] == 1

        with open(GEOJSON_PATH, "r") as geojson, patch(
            "app.utils.rw_api.get_geostore_geometry",
            return_value=json.load(geojson)["features"][0]["geometry"],
        ):
            response = await async_client.get(
                f"/dataset/{dataset}/{version}/query?sql=SELECT count(*) FROM mytable&geostore_id=17076d5ea9f214a5bdb68cc40433addb&geostore_origin=rw"
            )
        print(response.json())
        assert response.status_code == 200
        assert len(response.json()["data"]) == 1
        assert response.json()["data"][0]["count"] == 1

        with open(GEOJSON_PATH2, "r") as geojson, patch(
            "app.utils.rw_api.get_geostore_geometry",
            return_value=json.load(geojson)["features"][0]["geometry"],
        ):
            response = await async_client.get(
                f"/dataset/{dataset}/{version}/query?sql=SELECT count(*) FROM mytable&geostore_id=17076d5ea9f214a5bdb68cc40433addb&geostore_origin=rw"
            )
        print(response.json())
        assert response.status_code == 200
        assert len(response.json()["data"]) == 1
        assert response.json()["data"][0]["count"] == 0

        response = await async_client.get(
            f"/dataset/{dataset}/{version}/query?sql=select current_catalog from mytable;"
        )
        assert response.status_code == 400

        response = await async_client.get(
            f"/dataset/{dataset}/{version}/query?sql=select version() from mytable;"
        )
        assert response.status_code == 400

        response = await async_client.get(
            f"/dataset/{dataset}/{version}/query?sql=select has_any_column_privilege() from mytable;"
        )
        assert response.status_code == 400

        response = await async_client.get(
            f"/dataset/{dataset}/{version}/query?sql=select format_type() from mytable;"
        )
        assert response.status_code == 400

        response = await async_client.get(
            f"/dataset/{dataset}/{version}/query?sql=select col_description() from mytable;"
        )
        assert response.status_code == 400

        response = await async_client.get(
            f"/dataset/{dataset}/{version}/query?sql=select txid_current() from mytable;"
        )
        assert response.status_code == 400

        response = await async_client.get(
            f"/dataset/{dataset}/{version}/query?sql=select current_setting() from mytable;"
        )
        assert response.status_code == 400

        response = await async_client.get(
            f"/dataset/{dataset}/{version}/query?sql=select pg_cancel_backend() from mytable;"
        )
        assert response.status_code == 400

        response = await async_client.get(
            f"/dataset/{dataset}/{version}/query?sql=select brin_summarize_new_values() from mytable;"
        )
        assert response.status_code == 400

        response = await async_client.get(
            f"/dataset/{dataset}/{version}/query?sql=select doesnotexist() from mytable;"
        )
        assert response.status_code == 400

        # Stats
        # TODO: We currently don't compute stats, will need update this test once feature is available

        response = await async_client.get(f"/dataset/{dataset}/{version}/stats")
        print(response.json())
        assert response.status_code == 200
        assert response.json()["data"] is None

        # Fields
        response = await async_client.get(f"/dataset/{dataset}/{version}/fields")
        assert response.status_code == 200
        if i == 0:
            assert response.json()["data"] == [
                {
                    "field_name": "gfw_fid",
                    "field_alias": "gfw_fid",
                    "field_description": None,
                    "field_type": "integer",
                    "is_feature_info": True,
                    "is_filter": True,
                },
                {
                    "field_name": "fid",
                    "field_alias": "fid",
                    "field_description": None,
                    "field_type": "numeric",
                    "is_feature_info": True,
                    "is_filter": True,
                },
                {
                    "field_name": "geom",
                    "field_alias": "geom",
                    "field_description": None,
                    "field_type": "geometry",
                    "is_feature_info": False,
                    "is_filter": False,
                },
                {
                    "field_name": "geom_wm",
                    "field_alias": "geom_wm",
                    "field_description": None,
                    "field_type": "geometry",
                    "is_feature_info": False,
                    "is_filter": False,
                },
                {
                    "field_name": "gfw_area__ha",
                    "field_alias": "gfw_area__ha",
                    "field_description": None,
                    "field_type": "numeric",
                    "is_feature_info": True,
                    "is_filter": True,
                },
                {
                    "field_name": "gfw_geostore_id",
                    "field_alias": "gfw_geostore_id",
                    "field_description": None,
                    "field_type": "uuid",
                    "is_feature_info": True,
                    "is_filter": True,
                },
                {
                    "field_name": "gfw_geojson",
                    "field_alias": "gfw_geojson",
                    "field_description": None,
                    "field_type": "text",
                    "is_feature_info": False,
                    "is_filter": False,
                },
                {
                    "field_name": "gfw_bbox",
                    "field_alias": "gfw_bbox",
                    "field_description": None,
                    "field_type": "ARRAY",
                    "is_feature_info": False,
                    "is_filter": False,
                },
                {
                    "field_name": "created_on",
                    "field_alias": "created_on",
                    "field_description": None,
                    "field_type": "timestamp without time zone",
                    "is_feature_info": True,
                    "is_filter": True,
                },
                {
                    "field_name": "updated_on",
                    "field_alias": "updated_on",
                    "field_description": None,
                    "field_type": "timestamp without time zone",
                    "is_feature_info": True,
                    "is_filter": True,
                },
            ]
        else:
            # JSON file does not have fid field
            assert response.json()["data"] == [
                {
                    "field_name": "gfw_fid",
                    "field_alias": "gfw_fid",
                    "field_description": None,
                    "field_type": "integer",
                    "is_feature_info": True,
                    "is_filter": True,
                },
                {
                    "field_name": "geom",
                    "field_alias": "geom",
                    "field_description": None,
                    "field_type": "geometry",
                    "is_feature_info": False,
                    "is_filter": False,
                },
                {
                    "field_name": "geom_wm",
                    "field_alias": "geom_wm",
                    "field_description": None,
                    "field_type": "geometry",
                    "is_feature_info": False,
                    "is_filter": False,
                },
                {
                    "field_name": "gfw_area__ha",
                    "field_alias": "gfw_area__ha",
                    "field_description": None,
                    "field_type": "numeric",
                    "is_feature_info": True,
                    "is_filter": True,
                },
                {
                    "field_name": "gfw_geostore_id",
                    "field_alias": "gfw_geostore_id",
                    "field_description": None,
                    "field_type": "uuid",
                    "is_feature_info": True,
                    "is_filter": True,
                },
                {
                    "field_name": "gfw_geojson",
                    "field_alias": "gfw_geojson",
                    "field_description": None,
                    "field_type": "text",
                    "is_feature_info": False,
                    "is_filter": False,
                },
                {
                    "field_name": "gfw_bbox",
                    "field_alias": "gfw_bbox",
                    "field_description": None,
                    "field_type": "ARRAY",
                    "is_feature_info": False,
                    "is_filter": False,
                },
                {
                    "field_name": "created_on",
                    "field_alias": "created_on",
                    "field_description": None,
                    "field_type": "timestamp without time zone",
                    "is_feature_info": True,
                    "is_filter": True,
                },
                {
                    "field_name": "updated_on",
                    "field_alias": "updated_on",
                    "field_description": None,
                    "field_type": "timestamp without time zone",
                    "is_feature_info": True,
                    "is_filter": True,
                },
            ]

        requests.delete(f"http://localhost:{PORT}")

    response = await async_client.get(f"/asset/{asset_id}")
    assert response.status_code == 200

    response = await async_client.get("/dataset/different/v1.1.1/assets")
    assert response.status_code == 400

    response = await async_client.delete(f"/asset/{asset_id}")
    assert response.status_code == 409
    print(response.json())
    assert (
        response.json()["message"]
        == "Deletion failed. You cannot delete a default asset. To delete a default asset you must delete the parent version."
    )
