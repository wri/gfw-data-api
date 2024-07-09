import json

import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient

from app.routes.jobs import job

TEST_JOB_ID = "f3caa6c8-09d7-43a8-823f-e7528344a169"


def _get_sfn_execution_mocked_pending(job_id):
    return {
        "executionArn": "arn::fake_execution_arn",
        "stateMachineArn": "arn::fake_state_machine_arn",
        "status": "RUNNING",
        "input": json.dumps({"job_id": TEST_JOB_ID}),
        "output": None,
        "mapRunArn": "arn::fake_map_run_arn",
    }


def _get_sfn_execution_mocked_success(job_id):
    return {
        "executionArn": "arn::fake_execution_arn",
        "stateMachineArn": "arn::fake_state_machine_arn",
        "status": "SUCCEEDED",
        "input": json.dumps({"job_id": TEST_JOB_ID}),
        "output": json.dumps(
            {
                "job_id": TEST_JOB_ID,
                "status": "saved",
                "download_link": "s3://test/results.csv",
            }
        ),
        "mapRunArn": "arn::fake_map_run_arn",
    }


def _get_sfn_execution_mocked_failed(job_id):
    return {
        "executionArn": "arn::fake_execution_arn",
        "stateMachineArn": "arn::fake_state_machine_arn",
        "status": "FAILED",
        "input": json.dumps({"job_id": TEST_JOB_ID}),
        "output": None,
        "mapRunArn": "arn::fake_map_run_arn",
    }


def _get_map_run_mocked(job_id):
    return {
        "executionArn": "arn::fake_execution_arn",
        "mapRunArn": "arn::fake_map_run_arn",
        "itemCounts": {
            "succeeded": 100,
            "total": 1000,
        },
    }


@pytest.mark.asyncio
async def test_job_pending(
    async_client: AsyncClient,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(job, "_get_sfn_execution", _get_sfn_execution_mocked_pending)
    monkeypatch.setattr(job, "_get_map_run", _get_map_run_mocked)

    resp = await async_client.get(f"job/{TEST_JOB_ID}")

    assert resp.status_code == 200
    data = resp.json()["data"]

    assert data["job_id"] == TEST_JOB_ID
    assert data["status"] == "pending"
    assert data["download_link"] is None
    assert data["progress"] == "10%"


@pytest.mark.asyncio
async def test_job_success(
    async_client: AsyncClient,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(job, "_get_sfn_execution", _get_sfn_execution_mocked_success)

    resp = await async_client.get(f"job/{TEST_JOB_ID}")

    assert resp.status_code == 200
    data = resp.json()["data"]

    assert data["job_id"] == TEST_JOB_ID
    assert data["status"] == "saved"
    assert data["download_link"] == "s3://test/results.csv"
    assert data["progress"] == "100%"


@pytest.mark.asyncio
async def test_job_failed(
    async_client: AsyncClient,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(job, "_get_sfn_execution", _get_sfn_execution_mocked_failed)

    resp = await async_client.get(f"job/{TEST_JOB_ID}")

    assert resp.status_code == 200
    data = resp.json()["data"]

    assert data["job_id"] == TEST_JOB_ID
    assert data["status"] == "failed"
    assert data["download_link"] is None
    assert data["progress"] is None
