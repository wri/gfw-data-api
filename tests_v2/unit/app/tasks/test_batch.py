import asyncio
from typing import Any, Coroutine, Dict, List
from unittest.mock import MagicMock, patch
from uuid import UUID

from fastapi.logger import logger

from app.models.pydantic.change_log import ChangeLog
from app.tasks.batch import submit_batch_job
from app.tasks.vector_source_assets import _create_add_gfw_fields_job

TEST_JOB_ENV: List[Dict[str, str]] = [{"name": "PASSWORD", "value": "DON'T LOG ME"}]


async def mock_callback(uuid: UUID, changelog: ChangeLog):
    async def helper_function() -> Coroutine[Any, Any, None]:
        # Simulate some asynchronous work
        return asyncio.sleep(0)

    return helper_function()


@patch("app.utils.aws.boto3.client")
@patch.object(logger, "info")  # Patch the logger.info directly
@patch("app.tasks.batch.UUID")  # Patch the UUID class
async def test_submit_batch_job(mock_uuid, mock_logging_info, mock_boto3_client):
    mock_client = MagicMock()
    mock_boto3_client.return_value = mock_client

    attempt_duration_seconds: int = 100

    job = await _create_add_gfw_fields_job(
        "some_dataset",
        "v1",
        parents=list(),
        job_env=TEST_JOB_ENV,
        callback=mock_callback,
        attempt_duration_seconds=attempt_duration_seconds,
    )

    # Call the function you want to test
    submit_batch_job(job)

    mock_boto3_client.assert_called_once_with(
        "batch", region_name="us-east-1", endpoint_url=None
    )

    # Assert that the logger.info was called with the expected log message
    assert "add_gfw_fields" in mock_logging_info.call_args.args[0]
    assert "DON'T LOG ME" not in mock_logging_info.call_args.args[0]
