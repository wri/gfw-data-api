import json

import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient

from app.routes.jobs import job

TEST_JOB_ID = "f3caa6c8-09d7-43a8-823f-e7528344a169"


async def _get_sfn_execution_mocked_pending(job_id):
    return {
        "executionArn": "arn::fake_execution_arn",
        "stateMachineArn": "arn::fake_state_machine_arn",
        "status": "RUNNING",
        "input": json.dumps({"job_id": TEST_JOB_ID}),
        "output": None,
        "mapRunArn": "arn::fake_map_run_arn",
    }


async def _get_sfn_execution_mocked_success(job_id):
    return {
        "executionArn": "arn::fake_execution_arn",
        "stateMachineArn": "arn::fake_state_machine_arn",
        "status": "SUCCEEDED",
        "input": json.dumps({"job_id": TEST_JOB_ID}),
        "output": json.dumps(
            {
                "data": {
                    "job_id": TEST_JOB_ID,
                    "download_link": "s3://test/results.csv",
                    "failed_geometries_link": None,
                },
                "status": "success",
            }
        ),
        "mapRunArn": "arn::fake_map_run_arn",
    }


async def _get_sfn_execution_mocked_failed(job_id):
    return {
        "executionArn": "arn::fake_execution_arn",
        "stateMachineArn": "arn::fake_state_machine_arn",
        "status": "FAILED",
        "input": json.dumps({"job_id": TEST_JOB_ID}),
        "output": None,
        "mapRunArn": "arn::fake_map_run_arn",
    }


async def _get_sfn_execution_mocked_partial_success(job_id):
    return {
        "executionArn": "arn::fake_execution_arn",
        "stateMachineArn": "arn::fake_state_machine_arn",
        "status": "SUCCEEDED",
        "input": json.dumps({"job_id": TEST_JOB_ID}),
        "output": json.dumps(
            {
                "data": {
                    "job_id": TEST_JOB_ID,
                    "download_link": "s3://test/results.csv",
                    "failed_geometries_link": "s3://test/results_failed.csv",
                },
                "status": "partial_success",
            }
        ),
        "mapRunArn": "arn::fake_map_run_arn",
    }


async def _get_sfn_execution_mocked_failed_geoms(job_id):
    return {
        "executionArn": "arn::fake_execution_arn",
        "stateMachineArn": "arn::fake_state_machine_arn",
        "status": "SUCCEEDED",
        "input": json.dumps({"job_id": TEST_JOB_ID}),
        "output": json.dumps(
            {
                "data": {
                    "job_id": TEST_JOB_ID,
                    "download_link": None,
                    "failed_geometries_link": "s3://test/results_failed.csv",
                },
                "status": "failed",
            }
        ),
        "mapRunArn": "arn::fake_map_run_arn",
    }


async def _get_map_run_mocked_partial(job_id):
    return {
        "executionArn": "arn::fake_execution_arn",
        "mapRunArn": "arn::fake_map_run_arn",
        "itemCounts": {
            "succeeded": 100,
            "total": 1000,
        },
    }


async def _get_map_run_mocked_all(job_id):
    return {
        "executionArn": "arn::fake_execution_arn",
        "mapRunArn": "arn::fake_map_run_arn",
        "itemCounts": {
            "succeeded": 1000,
            "total": 1000,
        },
    }


@pytest.mark.asyncio
async def test_job_pending(
    async_client: AsyncClient,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(job, "_get_sfn_execution", _get_sfn_execution_mocked_pending)
    monkeypatch.setattr(job, "_get_map_run", _get_map_run_mocked_partial)

    resp = await async_client.get(f"job/{TEST_JOB_ID}")

    assert resp.status_code == 200
    data = resp.json()["data"]

    assert data["status"] == "pending"
    assert data["download_link"] is None
    assert data["progress"] == "10%"


@pytest.mark.asyncio
async def test_job_success(
    async_client: AsyncClient,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(job, "_get_sfn_execution", _get_sfn_execution_mocked_success)
    monkeypatch.setattr(job, "_get_map_run", _get_map_run_mocked_all)

    resp = await async_client.get(f"job/{TEST_JOB_ID}")

    assert resp.status_code == 200
    data = resp.json()["data"]

    assert data["job_id"] == TEST_JOB_ID
    assert data["status"] == "success"
    assert "test/results.csv" in data["download_link"]
    assert data["failed_geometries_link"] is None
    assert data["progress"] == "100%"


@pytest.mark.asyncio
async def test_job_partial_success(
    async_client: AsyncClient,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        job, "_get_sfn_execution", _get_sfn_execution_mocked_partial_success
    )
    monkeypatch.setattr(job, "_get_map_run", _get_map_run_mocked_partial)

    resp = await async_client.get(f"job/{TEST_JOB_ID}")

    assert resp.status_code == 200
    data = resp.json()["data"]

    assert data["job_id"] == TEST_JOB_ID
    assert data["status"] == "partial_success"
    assert "test/results.csv" in data["download_link"]
    assert "test/results_failed.csv" in data["failed_geometries_link"]
    assert data["progress"] == "10%"


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
