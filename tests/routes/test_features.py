import json

import pendulum
import pytest
from httpx import AsyncClient
from pendulum.parsing.exceptions import ParserError

from app.application import app
from app.crud import tasks
from app.utils.aws import get_s3_client
from tests import BUCKET, TSV_NAME, TSV_PATH
from tests.routes import create_default_asset
from tests.tasks import poll_jobs


@pytest.mark.asyncio
async def test_features(batch_client, httpd):

    _, logs = batch_client
    # httpd_port = httpd.server_port

    ############################
    # Setup test
    ############################

    s3_client = get_s3_client()

    s3_client.create_bucket(Bucket=BUCKET)
    s3_client.upload_file(TSV_PATH, BUCKET, TSV_NAME)

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
        "source_type": "table",
        "source_uri": [f"s3://{BUCKET}/{TSV_NAME}"],
        "creation_options": {
            "create_dynamic_vector_tile_cache": True,
            "src_driver": "text",
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

    # Create default asset in mocked BATCH
    asset = await create_default_asset(
        dataset, version, dataset_metadata=input_data, version_metadata=input_data
    )
    asset_id = asset["asset_id"]

    tasks_rows = await tasks.get_tasks(asset_id)
    task_ids = [str(task.task_id) for task in tasks_rows]

    # make sure, all jobs completed
    status = await poll_jobs(task_ids)
    assert status == "saved"

    # Get the logs in case something went wrong
    # _print_logs(logs)

    ########################
    # Test features endpoint
    ########################

    # All jobs completed, but they couldn't update the task status. Set them all
    # to report success. This should allow the logic that fills out the metadata
    # fields to proceed.
    async with AsyncClient(app=app, base_url="http://test", trust_env=False) as ac:
        existing_tasks = await ac.get(f"/tasks/assets/{asset_id}")

        for task in existing_tasks.json()["data"]:
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
            patch_resp = await ac.patch(f"/tasks/{task['task_id']}", json=patch_payload)
            assert patch_resp.json()["status"] == "success"

    # Verify the status of asset + version have been updated to "saved"
    async with AsyncClient(app=app, base_url="http://test", trust_env=False) as ac:
        version_resp = await ac.get(f"/meta/{dataset}/{version}")
        assert version_resp.json()["data"]["status"] == "saved"

        asset_resp = await ac.get(f"/meta/{dataset}/{version}/assets/{asset_id}")
        assert asset_resp.json()["data"]["status"] == "saved"

        # Print out the asset too, because you're going to want to look at the
        # metadata['fields'] section. Null. Infuriating.
        print(json.dumps(json.loads(asset_resp.text), indent=2))

        # Verify that the features endpoint doesn't 500
        resp = await ac.get(f"/features/{dataset}/{version}?lat=50&lng=60&z=10")
        print(resp.json())
        assert resp.status_code == 200

        # More stuff, like using _assert_fields which someone graciously provided


def _assert_fields(field_list, field_schema):
    count = 0
    for field in field_list:
        for schema in field_schema:
            if (
                field["field_name_"] == schema["field_name"]
                and field["field_type"] == schema["field_type"]
            ):
                count += 1
        if field["field_name_"] in ["geom", "geom_wm", "gfw_geojson", "gfw_bbox"]:
            assert not field["is_filter"]
            assert not field["is_feature_info"]
        else:
            assert field["is_filter"]
            assert field["is_feature_info"]
    assert count == len(field_schema)


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


# async def _check_version_status(dataset, version):
#     row = await versions.get_version(dataset, version)
#
#     # in this test we don't set the final version status to saved or failed
#     assert row.status == "pending"
#
#     # in this test we only see the logs from background task, not from batch jobs
#     print(f"TABLE SOURCE VERSION LOGS: {row.change_log}")
#     assert len(row.change_log) == 1
#     assert row.change_log[0]["message"] == "Successfully scheduled batch jobs"
#
#
# async def _check_asset_status(dataset, version, nb_jobs, last_job_name):
#     rows = await assets.get_assets(dataset, version)
#     assert len(rows) == 1
#
#     # in this test we don't set the final asset status to saved or failed
#     assert rows[0].status == "pending"
#     assert rows[0].is_default is True
#
#     # in this test we only see the logs from background task, not from batch jobs
#     print(f"TABLE SOURCE ASSET LOGS: {rows[0].change_log}")
#     assert len(rows[0].change_log) == nb_jobs
#     assert rows[0].change_log[-1]["message"] == f"Scheduled job {last_job_name}"
