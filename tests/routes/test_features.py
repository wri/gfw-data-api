import json
from datetime import date, timedelta

import pytest

from app.application import ContextEngine, db
from tests import BUCKET, TSV_NAME
from tests.utils import create_default_asset, version_metadata


@pytest.mark.asyncio
async def test_features(async_client, batch_client):

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
                # Use date.fromisocalendar, not pendulum.parse, deliberately -- pendulum
                # 3.0.0 (required for Python 3.12) has a real, still-open upstream bug
                # where its ISO
                # week-date parser raises ParserError for arbitrary, otherwise-valid
                # week numbers (e.g. "2018-W18"), not just the legitimate "this year
                # has only 52 ISO weeks" case this try/except was written for -- see
                # github.com/python-pendulum/pendulum/issues/839 and /916. The except
                # below silently swallowed both cases identically, so a handful of
                # partitions went missing with no visible error until a row destined
                # for one of them failed to insert. date.fromisocalendar raises a
                # plain ValueError only for the genuine case (week 53 in a 52-week
                # year), so this loop no longer depends on pendulum's parser at all.
                start_date = date.fromisocalendar(year, week, 1)
                end_date = start_date + timedelta(days=7)
                partition_schema.append(
                    {
                        "partition_suffix": name,
                        "start_value": start_date.isoformat(),
                        "end_value": end_date.isoformat(),
                    }
                )

            except ValueError:
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
                    "name": "rspo_oil_palm__certification_status",
                    "data_type": "text",
                },
                {"name": "per_forest_concession__type", "data_type": "text"},
                {"name": "idn_forest_area__type", "data_type": "text"},
                {"name": "alert__count", "data_type": "integer"},
                {"name": "adm1", "data_type": "integer"},
                {"name": "adm2", "data_type": "integer"},
            ],
        },
        "metadata": version_metadata,
    }

    # Create default asset in mocked Batch
    asset = await create_default_asset(
        dataset,
        version,
        version_payload=input_data,
        async_client=async_client,
        logs=logs,
        execute_batch_jobs=True,
    )
    asset_id = asset["asset_id"]

    response = await async_client.get(f"/asset/{asset_id}")
    assert response.json()["data"]["status"] == "saved"

    ########################
    # Test features endpoint
    ########################

    async with ContextEngine("READ"):
        _ = await db.scalar(f"""SELECT COUNT(*) FROM "{dataset}"."{version}" """)

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
    assert resp.status_code == 422
    assert resp.json()["status"] == "failed"
    assert set(
        [json.dumps(msg, sort_keys=True) for msg in resp.json()["message"]]
    ) == set(json.dumps(msg, sort_keys=True) for msg in expected_messages)

    # TODO: Assert on the content of the fields in the features response
