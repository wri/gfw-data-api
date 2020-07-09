import json
from unittest.mock import patch

import pendulum
import pytest
from httpx import AsyncClient
from pendulum.parsing.exceptions import ParserError

from app.application import app
from app.crud import tasks
from tests import BUCKET, TSV_NAME
from tests.routes import create_default_asset, generate_uuid
from tests.tasks import poll_jobs


@pytest.mark.asyncio
async def test_features(async_client, batch_client, httpd):

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
    with patch("app.tasks.batch.submit_batch_job", side_effect=generate_uuid):
        asset = await create_default_asset(
            async_client,
            dataset,
            version,
            dataset_payload=input_data,
            version_payload=input_data,
        )
    asset_id = asset["asset_id"]

    tasks_rows = await tasks.get_tasks(asset_id)
    task_ids = [str(task.task_id) for task in tasks_rows]

    # Wait until all jobs have finished
    status = await poll_jobs(task_ids)
    assert status == "saved"

    # All jobs completed, but they couldn't update the task status. Set them all
    # to report success. This should allow the logic that fills out the metadata
    # fields to proceed.

    for task_id in task_ids:
        patch_payload = {
            "change_log": [
                {
                    "date_time": "2020-06-25 14:30:00",
                    "status": "success",
                    "message": "All finished!",
                    "detail": "None",
                }
            ]
        }
        patch_resp = await async_client.patch(f"/tasks/{task_id}", json=patch_payload)
        assert patch_resp.json()["status"] == "success"

    ########################
    # Test features endpoint
    ########################

    # Exact match, z > 9 (though see FIXME in app/routes/features/features.py)
    resp = await async_client.get(
        f"/dataset/{dataset}/{version}/features?lat=4.42813&lng=17.97655&z=10"
    )
    assert resp.status_code == 200
    assert len(resp.json()["data"]) == 1
    assert resp.json()["data"][0]["iso"] == "CAF"

    # Nearby match
    resp = await async_client.get(
        f"/dataset/{dataset}/{version}/features?lat=9.40645&lng=-3.3681&z=9"
    )
    assert resp.status_code == 200
    assert len(resp.json()["data"]) == 1
    assert resp.json()["data"][0]["iso"] == "CIV"

    # No match
    resp = await async_client.get(
        f"/dataset/{dataset}/{version}/features?lat=10&lng=-10&z=22"
    )
    assert resp.status_code == 200
    assert len(resp.json()["data"]) == 0

    # Invalid latitude, longitude, or zoom level
    # Check all the constraints at once, why not?
    expected_messages = [
        {
            "loc": ["query", "lat"],
            "msg": "ensure this value is less than or equal to 90",
            "type": "value_error.number.not_le",
            "ctx": {"limit_value": 90},
        },
        {
            "loc": ["query", "lng"],
            "msg": "ensure this value is less than or equal to 180",
            "type": "value_error.number.not_le",
            "ctx": {"limit_value": 180},
        },
        {
            "loc": ["query", "z"],
            "msg": "ensure this value is less than or equal to 22",
            "type": "value_error.number.not_le",
            "ctx": {"limit_value": 22},
        },
    ]
    resp = await async_client.get(
        f"/dataset/{dataset}/{version}/features?lat=360&lng=360&z=25"
    )

    assert resp.status_code == 422
    assert resp.json()["status"] == "failed"
    assert set(
        [json.dumps(msg, sort_keys=True) for msg in resp.json()["message"]]
    ) == set(json.dumps(msg, sort_keys=True) for msg in expected_messages)

    # Invalid latitude, longitude, or zoom level, opposite limits
    # Check all the constraints at once, why not?
    expected_messages = [
        {
            "loc": ["query", "lat"],
            "msg": "ensure this value is greater than or equal to -90",
            "type": "value_error.number.not_ge",
            "ctx": {"limit_value": -90},
        },
        {
            "loc": ["query", "lng"],
            "msg": "ensure this value is greater than or equal to -180",
            "type": "value_error.number.not_ge",
            "ctx": {"limit_value": -180},
        },
        {
            "loc": ["query", "z"],
            "msg": "ensure this value is greater than or equal to 0",
            "type": "value_error.number.not_ge",
            "ctx": {"limit_value": 0},
        },
    ]
    resp = await async_client.get(
        f"/dataset/{dataset}/{version}/features?lat=-360&lng=-360&z=-1"
    )
    print(resp.json())
    assert resp.status_code == 422
    assert resp.json()["status"] == "failed"
    assert set(
        [json.dumps(msg, sort_keys=True) for msg in resp.json()["message"]]
    ) == set(json.dumps(msg, sort_keys=True) for msg in expected_messages)

    # TODO: Assert on the content of the fields in the features response
