from typing import List
from uuid import UUID

import pytest
from httpx import AsyncClient

from app.application import ContextEngine, db
from app.models.orm.geostore import Geostore
from app.models.pydantic.geostore import GeostoreResponse
from tests import BUCKET, GEOJSON_NAME
from tests.utils import create_default_asset, version_metadata


@pytest.mark.asyncio
async def test_user_area_geostore(async_client: AsyncClient):
    # This is the gfw_geostore_id returned when POSTing the payload with Postman
    expected_goestore_id = "b44a9213-4fc2-14e6-02e3-96faf0d89499"

    # Start off by checking the response for a geostore that doesn't exist
    resp = await async_client.get(f"/geostore/{expected_goestore_id}")
    assert resp.status_code == 404
    assert resp.json() == {
        "status": "failed",
        "message": f"Area with gfw_geostore_id {expected_goestore_id} does not exist",
    }

    payload = {
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [[[[8, 51], [11, 55], [12, 49], [8, 51]]]],
        }
    }
    # POST the new geostore
    post_resp = await async_client.post(
        "/geostore", json=payload, follow_redirects=True
    )
    assert post_resp.status_code == 201
    assert post_resp.json()["data"]["gfw_geostore_id"] == expected_goestore_id
    # Validate response structure
    GeostoreResponse.parse_raw(post_resp.text)

    # Do our GET again, which should now find the POSTed user area. Ensure
    # the GET response is identical to the POST response
    # (except for status code)
    get_resp = await async_client.get(f"/geostore/{expected_goestore_id}")
    assert get_resp.status_code == 200
    assert get_resp.json() == post_resp.json()

    # POSTing the same payload (more specifically geometry) again should yield
    # the same response (we're squashing duplicate key errors)
    post_resp2 = await async_client.post(
        "/geostore", json=payload, follow_redirects=True
    )
    assert post_resp2.status_code == 201
    assert post_resp2.json() == post_resp.json()


@pytest.mark.asyncio
async def test_dataset_version_geostore(async_client: AsyncClient, batch_client):
    _, logs = batch_client

    ############################
    # Setup test
    ############################

    dataset = "test"
    source = GEOJSON_NAME
    version = "v1.1.1"
    input_data = {
        "creation_options": {
            "source_type": "vector",
            "source_uri": [f"s3://{BUCKET}/{source}"],
            "source_driver": "GeoJSON",
            "create_dynamic_vector_tile_cache": True,
        },
        "metadata": version_metadata,
    }

    _ = await create_default_asset(
        dataset,
        version,
        version_payload=input_data,
        async_client=async_client,
        logs=logs,
        execute_batch_jobs=True,
        skip_dataset=False,
    )

    # There should be a table called "test"."v1.1.1" with one row
    async with ContextEngine("READ"):
        count = await db.scalar(db.text(f'SELECT count(*) FROM {dataset}."{version}"'))
    assert count == 1

    ############################
    # Test geostore endpoints
    ############################

    # This is the hash obtained by POSTing the sample GeoJSON with Postman
    sample_geojson_hash = "41b67a74-4ea2-df3f-c3f3-d7131a645f9a"

    # The geometry should be accessible via the geostore table
    async with ContextEngine("READ"):
        rows: List[Geostore] = await Geostore.query.gino.all()
    assert len(rows) == 1
    assert rows[0].gfw_geostore_id == UUID(sample_geojson_hash)

    # The geostore should be accessible with its hash via the geostore endpoint
    resp = await async_client.get(f"/geostore/{sample_geojson_hash}")
    # Validate response structure
    GeostoreResponse.parse_raw(resp.text)

    # ...and via the dataset + version-specific endpoint
    resp_by_version = await async_client.get(
        f"/dataset/{dataset}/{version}/geostore/{sample_geojson_hash}"
    )
    # Validate response structure
    GeostoreResponse.parse_raw(resp_by_version.text)

    # If we POST a user area there should then be two geostore records
    # The new one should not be findable via the dataset.version
    # endpoint. Let's test that.
    payload = {
        "geometry": {
            "type": "MultiPolygon",
            "coordinates": [[[[8, 51], [11, 55], [12, 49], [8, 51]]]],
        }
    }
    # This is the gfw_geostore_id returned when POSTing the payload with Postman
    second_sample_geojson_hash = "b44a9213-4fc2-14e6-02e3-96faf0d89499"

    # Create the new geostore record
    post_resp = await async_client.post(
        "/geostore", json=payload, follow_redirects=True
    )
    assert post_resp.status_code == 201
    assert post_resp.json()["data"]["gfw_geostore_id"] == second_sample_geojson_hash

    # The second geometry should be accessible via the geostore table
    async with ContextEngine("READ"):
        rows = await Geostore.query.gino.all()
    assert len(rows) == 2

    # ... but it should not be visible in the dataset.version child table
    get_resp = await async_client.get(
        f"/dataset/{dataset}/{version}/geostore/{second_sample_geojson_hash}"
    )
    assert get_resp.status_code == 404
    assert get_resp.json() == {
        "status": "failed",
        "message": f'Area with gfw_geostore_id {second_sample_geojson_hash} does not exist in "{dataset}"."{version}"',
    }


@pytest.mark.asyncio
async def test_user_area_geostore_bad_requests(async_client: AsyncClient, batch_client):
    # Try POSTing a geostore with no features, multiple features
    bad_payload_1 = {"type": "FeatureCollection", "features": []}
    bad_payload_2 = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [[[[8, 51], [11, 55], [12, 49], [8, 51]]]],
                },
            },
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [[[[8, 51], [11, 55], [12, 49], [8, 51]]]],
                },
            },
        ],
    }
    # expected_message = "Please submit one and only one feature per request"

    post_resp = await async_client.post(
        "/geostore", json=bad_payload_1, follow_redirects=True
    )
    assert post_resp.status_code == 422
    # assert post_resp.json()["message"] == expected_message

    post_resp = await async_client.post(
        "/geostore", json=bad_payload_2, follow_redirects=True
    )
    assert post_resp.status_code == 422
    # assert post_resp.json()["message"] == expected_message
