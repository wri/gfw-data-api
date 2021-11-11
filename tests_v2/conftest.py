import json
import os
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, Tuple

import pytest
from _pytest.monkeypatch import MonkeyPatch
from alembic.config import main
from fastapi.testclient import TestClient
from httpx import AsyncClient

from app.authentication.token import get_user, is_admin, is_service_account
from app.models.enum.change_log import ChangeLogStatus
from app.models.pydantic.change_log import ChangeLog
from app.routes.datasets import versions
from app.tasks import batch, delete_assets, revision_assets, vector_source_assets
from app.tasks.raster_tile_set_assets import raster_tile_set_assets
from tests_v2.fixtures.creation_options.versions import (
    RASTER_CREATION_OPTIONS,
    REVISION_CREATION_OPTIONS,
    VECTOR_SOURCE_CREATION_OPTIONS,
)
from tests_v2.utils import (
    BatchJobMock,
    _create_vector_revision_assets,
    _create_vector_source_assets,
    bool_function_closure,
    dict_function_closure,
    get_admin_mocked,
    get_extent_mocked,
    get_user_mocked,
    int_function_closure,
    void_coroutine,
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

    # mock authentication function to avoid having to reach out to RW API during tests
    app.dependency_overrides[is_admin] = bool_function_closure(True, with_args=False)
    app.dependency_overrides[is_service_account] = bool_function_closure(
        True, with_args=False
    )
    app.dependency_overrides[get_user] = get_admin_mocked

    async with AsyncClient(app=app, base_url="http://test", trust_env=False) as client:
        yield client

    # Clean up
    app.dependency_overrides = {}


@pytest.fixture()
@pytest.mark.asyncio
async def async_client_unauthenticated(
    db, init_db
) -> AsyncGenerator[AsyncClient, None]:
    """Async Test Client."""
    from app.main import app

    async with AsyncClient(app=app, base_url="http://test", trust_env=False) as client:
        yield client

    # Clean up
    app.dependency_overrides = {}


@pytest.fixture()
@pytest.mark.asyncio
async def async_client_no_admin(db, init_db) -> AsyncGenerator[AsyncClient, None]:
    """Async Test Client."""
    from app.main import app

    # mock authentication function to avoid having to reach out to RW API during tests
    app.dependency_overrides[is_admin] = bool_function_closure(False, with_args=False)
    app.dependency_overrides[is_service_account] = bool_function_closure(
        False, with_args=False
    )
    app.dependency_overrides[get_user] = get_user_mocked

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
    version_metadata: Dict[str, Any] = {
        "title": "original",
        "content_date": "2021-09-28",
    }

    # patch all functions which reach out to external services
    batch_job_mock = BatchJobMock()
    monkeypatch.setattr(versions, "_verify_source_file_access", void_coroutine)
    monkeypatch.setattr(batch, "submit_batch_job", batch_job_mock.submit_batch_job)
    monkeypatch.setattr(vector_source_assets, "is_zipped", bool_function_closure(False))
    monkeypatch.setattr(delete_assets, "delete_s3_objects", int_function_closure(1))
    monkeypatch.setattr(versions, "flush_cloudfront_cache", dict_function_closure({}))
    monkeypatch.setattr(
        delete_assets, "flush_cloudfront_cache", dict_function_closure({})
    )

    # Create version
    response = await async_client.put(
        f"/dataset/{dataset_name}/{version_name}",
        json={
            "metadata": version_metadata,
            "creation_options": VECTOR_SOURCE_CREATION_OPTIONS,
        },
    )

    assert response.status_code == 202

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
async def generic_raster_version(
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
    monkeypatch.setattr(versions, "_verify_source_file_access", void_coroutine)
    monkeypatch.setattr(batch, "submit_batch_job", batch_job_mock.submit_batch_job)
    monkeypatch.setattr(vector_source_assets, "is_zipped", bool_function_closure(False))
    monkeypatch.setattr(delete_assets, "delete_s3_objects", int_function_closure(1))
    monkeypatch.setattr(raster_tile_set_assets, "get_extent", get_extent_mocked)
    monkeypatch.setattr(
        delete_assets, "flush_cloudfront_cache", dict_function_closure({})
    )

    # Create version
    await async_client.put(
        f"/dataset/{dataset_name}/{version_name}",
        json={
            "metadata": version_metadata,
            "creation_options": RASTER_CREATION_OPTIONS,
        },
    )

    # mock batch processes
    # TODO need to add anything here?
    # await _create_vector_source_assets(dataset_name, version_name)

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
async def generic_vector_revision(
    async_client: AsyncClient,
    generic_dataset: Tuple[str, str],
    monkeypatch: MonkeyPatch,
) -> AsyncGenerator[Tuple[str, str, Dict[str, Any]], None]:
    """Create generic vector source revision."""

    dataset_name, _ = generic_dataset
    version_name: str = "v2"
    version_metadata: Dict[str, Any] = {
        "title": "overwrite",
        "last_update": "2021-10-28",
    }

    # patch all functions which reach out to external services
    batch_job_mock = BatchJobMock()
    monkeypatch.setattr(versions, "_verify_source_file_access", void_coroutine)
    monkeypatch.setattr(batch, "submit_batch_job", batch_job_mock.submit_batch_job)
    monkeypatch.setattr(revision_assets, "is_zipped", bool_function_closure(False))
    monkeypatch.setattr(delete_assets, "delete_s3_objects", int_function_closure(1))
    monkeypatch.setattr(versions, "flush_cloudfront_cache", dict_function_closure({}))
    monkeypatch.setattr(
        delete_assets, "flush_cloudfront_cache", dict_function_closure({})
    )

    # Create version
    response = await async_client.put(
        f"/dataset/{dataset_name}/{version_name}",
        json={
            "metadata": version_metadata,
            "creation_options": REVISION_CREATION_OPTIONS,
        },
    )

    assert response.status_code == 202

    # mock batch processes
    await _create_vector_revision_assets(
        dataset_name, version_name, REVISION_CREATION_OPTIONS["revision_on"]
    )

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
async def apikey(
    async_client: AsyncClient,
) -> AsyncGenerator[Tuple[str, Dict[str, Any]], None]:

    # Get API Key
    payload = {
        "alias": "test",
        "organization": "Global Forest Watch",
        "email": "admin@globalforestwatch.org",
        "domains": ["www.globalforestwatch.org"],
        "never_expires": False,
    }

    response = await async_client.post("/auth/apikey", json=payload)
    api_key = response.json()["data"]["api_key"]

    # yield api key and associated origin
    yield api_key, payload

    # Clean up
    await async_client.delete(f"/auth/apikey/{api_key}")


@pytest.fixture()
@pytest.mark.asyncio()
async def apikey_unrestricted(
    async_client: AsyncClient,
) -> AsyncGenerator[Tuple[str, Dict[str, Any]], None]:

    # Get API Key
    payload = {
        "alias": "unrestricted",
        "organization": "Global Forest Watch",
        "email": "admin@globalforestwatch.org",
        "domains": [],
        "never_expires": True,
    }

    response = await async_client.post("/auth/apikey", json=payload)
    api_key = response.json()["data"]["api_key"]

    # yield api key and associated origin
    yield api_key, payload

    # Clean up
    await async_client.delete(f"/auth/apikey/{api_key}")


@pytest.fixture()
def geojson():
    return _load_geojson("test")


@pytest.fixture()
def geojson_huge():
    return _load_geojson("test_huge")


def _load_geojson(name):
    with open(f"{os.path.dirname(__file__)}/fixtures/geojson/{name}.geojson") as src:
        geojson = json.load(src)

    return geojson


@pytest.fixture()
@pytest.mark.asyncio()
async def geostore_huge(
    async_client: AsyncClient, geojson_huge
) -> AsyncGenerator[str, None]:
    # Get geostore ID
    geostore_id = await _create_geostore(geojson_huge, async_client)

    yield geostore_id

    # Clean up
    # Nothing to do here. No clean up function for geostore_huge.


@pytest.fixture()
@pytest.mark.asyncio()
async def geostore(async_client: AsyncClient, geojson) -> AsyncGenerator[str, None]:
    # Get geostore ID
    geostore_id = await _create_geostore(geojson, async_client)

    yield geostore_id

    # Clean up
    # Nothing to do here. No clean up function for geostore.


async def _create_geostore(geojson: Dict[str, Any], async_client: AsyncClient) -> str:
    payload = {
        "geometry": geojson["features"][0]["geometry"],
    }

    response = await async_client.post("/geostore", json=payload)
    assert response.status_code == 201

    return response.json()["data"]["gfw_geostore_id"]


@pytest.fixture()
@pytest.mark.asyncio()
async def version_alias(
    async_client: AsyncClient,
    generic_vector_source_version: Tuple[str, str, Dict[str, Any]],
) -> AsyncGenerator[Tuple[str, str, str], None]:

    dataset_name, version_name, _ = generic_vector_source_version
    alias = "v20151213"
    response = await async_client.put(
        f"/alias/version/{dataset_name}/{alias}", json={"version": version_name}
    )
    assert response.status_code == 200

    # Yield version alias
    yield dataset_name, version_name, response.json()["data"]["alias"]

    # Clean up
    await async_client.delete(f"/alias/version/{dataset_name}/{alias}")
