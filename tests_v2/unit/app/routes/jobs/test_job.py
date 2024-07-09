import json

import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient

from app.routes.jobs import job

TEST_JOB_ID = "f3caa6c8-09d7-43a8-823f-e7528344a169"


def _get_sfn_execution_mocked(job_id):
    return {
        "executionArn": "arn::fake_execution_arn",
        "stateMachineArn": "arn::fake_state_machine_arn",
        "status": "RUNNING",
        "input": json.dumps({"job_id": TEST_JOB_ID}),
        "outputt": json.dumps(
            {"job_id": TEST_JOB_ID, "download_link": "s3://test/results.csv"}
        ),
        "mapRunArn": "arn::fake_map_run_arn",
    }


@pytest.mark.asyncio
async def test_job_pending(
    async_client: AsyncClient,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(job, "_get_sfn_execution", _get_sfn_execution_mocked)

    resp = await async_client.get(f"job/{TEST_JOB_ID}")

    assert resp.status_code == 200
    data = resp.json()["data"]

    assert data["job_id"] == TEST_JOB_ID
