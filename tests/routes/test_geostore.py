import json

import pendulum
import pytest
from pendulum.parsing.exceptions import ParserError

from app.application import ContextEngine, db
from app.models.pydantic.geostore import GeostoreResponse
from tests import BUCKET, TSV_NAME
from tests.utils import create_default_asset


@pytest.mark.asyncio
async def test_user_area_geostore(async_client):
    # This is the gfw_gestore_id returned when POSTing the payload with Postman
    expected_goestore_id = "b44a9213-4fc2-14e6-02e3-96faf0d89499"
    resp = await async_client.get(f"/geostore/{expected_goestore_id}")
    assert resp.status_code == 404
    assert resp.json() == {
        "status": "failed",
        "data": f"Area with gfw_geostore_id {expected_goestore_id} does not exist",
    }

    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [[[[8, 51], [11, 55], [12, 49], [8, 51]]]],
                },
            }
        ],
    }
    post_resp = await async_client.post("/geostore", json=payload)
    assert post_resp.status_code == 201
    assert post_resp.json()["data"]["gfw_geostore_id"] == expected_goestore_id
    # Validate response structure
    GeostoreResponse.parse_raw(post_resp.text)

    # Do our GET again, which should now find the POSTed user area. Ensure
    # the GET response content is identical to the POST response
    # (except for status code)
    get_resp = await async_client.get(f"/geostore/{expected_goestore_id}")
    assert get_resp.status_code == 200
    assert get_resp.json() == post_resp.json()

    # POSTing the same payload (more specifically geometry) again should yield
    # the same response (we're squashing duplicate key errors)
    post_resp2 = await async_client.post("/geostore", json=payload)
    assert post_resp2.status_code == 201
    assert post_resp2.json() == post_resp.json()


@pytest.mark.asyncio
async def test_dataset_version_geostore(async_client, batch_client):

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
            "create_dynamic_vector_tile_cache": True,
            "source_driver": "text",
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

    # Create default asset in mocked Batch
    asset = await create_default_asset(
        dataset,
        version,
        dataset_payload=input_data,
        version_payload=input_data,
        async_client=async_client,
        logs=logs,
        execute_batch_jobs=True,
    )
    asset_id = asset["asset_id"]

    response = await async_client.get(f"/asset/{asset_id}")
    assert response.json()["data"]["status"] == "saved"

    #########################
    # Test geostore endpoints
    #########################

    # Now we should have a row (or more) in child table of geostore

    # from time import sleep; sleep(600)

    # This is the gfw_gestore_id obtained by POSTing the sample GeoJSON with Postman
    sample_geojson_hash = "b9faa657-34c9-96d4-fce4-8bb8a1507cb3"

    resp1 = await async_client.get(
        f"/dataset/{dataset}/{version}/geostore/{sample_geojson_hash}"
    )
    print(f"GEOSTORE BY VERSION RESPONSE: {resp1.json()}")

    resp2 = await async_client.get(f"/geostore/{sample_geojson_hash}")
    print(f"GEOSTORE FROM ANYWHERE RESPONSE: {resp2.json()}")

    resp_all = await async_client.get("/geostores")
    print(f"ALL GEOSTORES RESPONSE: {resp_all.json()}")

    resp_all_by_ver = await async_client.get(f"/dataset/{dataset}/{version}/geostores")
    print(f"ALL GEOSTORES BY VERSION RESPONSE: {resp_all_by_ver.json()}")

    assert 1 == 2
