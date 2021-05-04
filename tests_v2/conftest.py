from datetime import datetime
from typing import Any, AsyncGenerator, Dict, Tuple

import pytest
from _pytest.monkeypatch import MonkeyPatch
from alembic.config import main
from fastapi.testclient import TestClient
from httpx import AsyncClient

from app.authentication.token import get_user_id, is_admin, is_service_account
from app.models.enum.change_log import ChangeLogStatus
from app.models.pydantic.change_log import ChangeLog
from app.routes.datasets import versions
from app.tasks import batch, delete_assets, vector_source_assets
from tests_v2.fixtures.creation_options.versions import VECTOR_SOURCE_CREATION_OPTIONS
from tests_v2.utils import (
    BatchJobMock,
    _create_vector_source_assets,
    dict_function_closure,
    false_function,
    get_user_id_mocked,
    int_function_closure,
    is_admin_mocked,
    is_service_account_mocked,
    void_function,
)


@pytest.fixture()
@pytest.mark.asyncio
async def db():
    """In between tests, tear down/set up all DBs."""
    main(["--raiseerr", "upgrade", "head"])
    yield
    main(["--raiseerr", "downgrade", "base"])


@pytest.fixture(scope="module")
def module_db():
    """make sure that the db is only initialized and teared down once per
    module."""
    main(["--raiseerr", "upgrade", "head"])
    yield

    main(["--raiseerr", "downgrade", "base"])


@pytest.fixture()
def init_db():
    """Initialize database.

    This fixture is necessary when testing database connections outside
    the test client.
    """
    from app.main import app

    # It is easiest to do this using the standard test client
    with TestClient(app):
        yield


@pytest.fixture()
@pytest.mark.asyncio
async def async_client(db, init_db) -> AsyncGenerator[AsyncClient, None]:
    """Async Test Client."""
    from app.main import app

    # mock authentification function to avoid having to reach out to RW API during tests
    app.dependency_overrides[is_admin] = is_admin_mocked
    app.dependency_overrides[is_service_account] = is_service_account_mocked
    app.dependency_overrides[get_user_id] = get_user_id_mocked
    # app.dependency_overrides[get_api_key] = get_api_key_mocked

    async with AsyncClient(app=app, base_url="http://test", trust_env=False) as client:
        yield client

    # Clean up
    app.dependency_overrides = {}


@pytest.fixture()
@pytest.mark.asyncio()
async def generic_dataset(
    async_client: AsyncClient,
) -> AsyncGenerator[Tuple[str, Dict[str, Any]], None]:
    """Create generic dataset."""

    # Create dataset
    dataset_name: str = "my_first_dataset"
    dataset_metadata: Dict[str, Any] = {}

    await async_client.put(
        f"/dataset/{dataset_name}", json={"metadata": dataset_metadata}
    )

    # Yield dataset name and associated metadata
    yield dataset_name, dataset_metadata

    # Clean up
    await async_client.delete(f"/dataset/{dataset_name}")


@pytest.fixture()
@pytest.mark.asyncio()
async def generic_vector_source_version(
    async_client: AsyncClient,
    generic_dataset: Tuple[str, str],
    monkeypatch: MonkeyPatch,
) -> AsyncGenerator[Tuple[str, str, Dict[str, Any]], None]:
    """Create generic vector source version."""

    dataset_name, _ = generic_dataset
    version_name: str = "v1"
    version_metadata: Dict[str, Any] = {}

    # patch all functions which reach out to external services
    batch_job_mock = BatchJobMock()
    monkeypatch.setattr(versions, "_verify_source_file_access", void_function)
    monkeypatch.setattr(batch, "submit_batch_job", batch_job_mock.submit_batch_job)
    monkeypatch.setattr(vector_source_assets, "is_zipped", false_function)
    monkeypatch.setattr(delete_assets, "delete_s3_objects", int_function_closure(1))
    monkeypatch.setattr(
        delete_assets, "flush_cloudfront_cache", dict_function_closure({})
    )

    # Create version
    await async_client.put(
        f"/dataset/{dataset_name}/{version_name}",
        json={
            "metadata": version_metadata,
            "creation_options": VECTOR_SOURCE_CREATION_OPTIONS,
        },
    )

    # mock batch processes
    await _create_vector_source_assets(dataset_name, version_name)

    # Set all pending tasks to success
    for job_id in batch_job_mock.jobs:
        payload = {
            "change_log": [
                ChangeLog(
                    date_time=datetime.now(),
                    status=ChangeLogStatus.success,
                    message="Job set to success via fixture",
                    detail="",
                ).dict()
            ]
        }

        # convert datetime obt to string
        payload["change_log"][0]["date_time"] = str(
            payload["change_log"][0]["date_time"]
        )
        await async_client.patch(f"/task/{job_id}", json=payload)

    # Assert that version is saved, just to make sure
    response = await async_client.get(f"/dataset/{dataset_name}/{version_name}")
    assert response.json()["data"]["status"] == "saved"

    # yield version
    yield dataset_name, version_name, version_metadata

    # clean up
    await async_client.delete(f"/dataset/{dataset_name}/{version_name}")


@pytest.fixture()
@pytest.mark.asyncio()
async def apikey(async_client: AsyncClient) -> AsyncGenerator[Tuple[str, str], None]:

    # Get API Key
    payload = {
        "alias": "test",
        "organization": "Global Forest Watch",
        "email": "admin@globalforestwatch.org",
        "domains": ["*.globalforestwatch.org"],
    }

    response = await async_client.post("/auth/apikey", json=payload)
    api_key = response.json()["data"]["api_key"]
    origin = "www.globalforestwatch.org"

    # yield api key and associated origin
    yield api_key, origin

    # Clean up
    await async_client.delete(f"auth/apikey/{api_key}")
