import json
import os
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, Tuple

import pytest
import pytest_asyncio
from _pytest.monkeypatch import MonkeyPatch
from alembic.config import main
from fastapi.testclient import TestClient
from httpx import AsyncClient

from app.authentication.token import get_user, is_admin, is_service_account
from app.crud import api_keys
from app.models.enum.change_log import ChangeLogStatus
from app.models.pydantic.change_log import ChangeLog
from app.routes.datasets import versions
from app.tasks import batch, delete_assets, vector_source_assets
from app.tasks.raster_tile_set_assets import raster_tile_set_assets
from tests_v2.fixtures.creation_options.versions import (
    RASTER_CREATION_OPTIONS,
    VECTOR_SOURCE_CREATION_OPTIONS,
)
from tests_v2.fixtures.metadata.dataset import DATASET_METADATA
from tests_v2.fixtures.metadata.version import VERSION_METADATA
from tests_v2.utils import (
    BatchJobMock,
    _create_vector_source_assets,
    bool_function_closure,
    dict_function_closure,
    get_admin_mocked,
    get_extent_mocked,
    get_user_mocked,
    int_function_closure,
    void_coroutine,
)


@pytest_asyncio.fixture
async def db():
    """In between tests, tear down/set up all DBs."""
    main(["--raiseerr", "upgrade", "head"])
    yield
    main(["--raiseerr", "downgrade", "base"])


@pytest.fixture(scope="module")
def module_db():
    """make sure that the db is only initialized and torn down once per
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


@pytest_asyncio.fixture
async def async_client(db, init_db) -> AsyncGenerator[AsyncClient, None]:
    """Async Test Client."""
    from app.main import app

    # mock authentication function to avoid having to reach out to RW API during tests
    app.dependency_overrides[is_admin] = bool_function_closure(True, with_args=False)
    app.dependency_overrides[is_service_account] = bool_function_closure(
        True, with_args=False
    )
    app.dependency_overrides[get_user] = get_admin_mocked

    async with AsyncClient(
        app=app,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as client:
        yield client

    # Clean up
    app.dependency_overrides = {}


@pytest_asyncio.fixture
async def async_client_unauthenticated(
    db, init_db
) -> AsyncGenerator[AsyncClient, None]:
    """Async Test Client."""
    from app.main import app

    async with AsyncClient(app=app, base_url="http://test", trust_env=False) as client:
        yield client

    # Clean up
    app.dependency_overrides = {}


@pytest_asyncio.fixture
async def async_client_no_admin(db, init_db) -> AsyncGenerator[AsyncClient, None]:
    """Async Test Client."""
    from app.main import app

    # mock authentication function to avoid having to reach out to RW API during tests
    app.dependency_overrides[is_admin] = bool_function_closure(False, with_args=False)
    app.dependency_overrides[is_service_account] = bool_function_closure(
        False, with_args=False
    )
    app.dependency_overrides[get_user] = get_user_mocked

    async with AsyncClient(
        app=app,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as client:
        yield client

    # Clean up
    app.dependency_overrides = {}


@pytest_asyncio.fixture
async def generic_dataset(
    async_client: AsyncClient,
) -> AsyncGenerator[Tuple[str, Dict[str, Any]], None]:
    """Create generic dataset."""

    # Create dataset
    dataset_name: str = "my_first_dataset"

    await async_client.put(
        f"/dataset/{dataset_name}", json={"metadata": DATASET_METADATA}
    )

    # Yield dataset name and associated metadata
    yield dataset_name, DATASET_METADATA

    # Clean up
    await async_client.delete(f"/dataset/{dataset_name}")


@pytest_asyncio.fixture
async def generic_vector_source_version(
    async_client: AsyncClient,
    generic_dataset: Tuple[str, str],
    monkeypatch: MonkeyPatch,
) -> AsyncGenerator[Tuple[str, str, Dict[str, Any]], None]:
    """Create generic vector source version."""

    dataset_name, _ = generic_dataset
    version_name: str = "v1"

    await create_vector_source_version(async_client, dataset_name, version_name, monkeypatch)

    # yield version
    yield dataset_name, version_name, VERSION_METADATA

    # clean up
    await async_client.delete(f"/dataset/{dataset_name}/{version_name}")


# Create a vector version, given the name of an existing dataset, plus a new version
# name.
async def create_vector_source_version(
    async_client: AsyncClient,
    dataset_name: str,
    version_name: str,
    monkeypatch: MonkeyPatch,
):
    """Create generic vector source version."""

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
            "metadata": VERSION_METADATA,
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

@pytest_asyncio.fixture
async def generic_raster_version(
    async_client: AsyncClient,
    generic_dataset: Tuple[str, str],
    monkeypatch: MonkeyPatch,
) -> AsyncGenerator[Tuple[str, str, Dict[str, Any]], None]:
    """Create generic raster source version."""

    dataset_name, _ = generic_dataset
    version_name: str = "v1"

    # patch all functions which reach out to external services
    batch_job_mock = BatchJobMock()
    monkeypatch.setattr(versions, "_verify_source_file_access", void_coroutine)
    monkeypatch.setattr(batch, "submit_batch_job", batch_job_mock.submit_batch_job)
    monkeypatch.setattr(delete_assets, "delete_s3_objects", int_function_closure(1))
    monkeypatch.setattr(raster_tile_set_assets, "get_extent", get_extent_mocked)
    monkeypatch.setattr(
        delete_assets, "flush_cloudfront_cache", dict_function_closure({})
    )

    # Create version
    await async_client.put(
        f"/dataset/{dataset_name}/{version_name}",
        json={
            "metadata": VERSION_METADATA,
            "creation_options": RASTER_CREATION_OPTIONS,
        },
    )
    await async_client.patch(
        f"/dataset/{dataset_name}/{version_name}",
        json={
            "is_latest": True,
        },
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
    yield dataset_name, version_name, VERSION_METADATA

    # clean up
    await async_client.delete(f"/dataset/{dataset_name}/{version_name}")

@pytest_asyncio.fixture
async def licensed_dataset(
    async_client: AsyncClient,
) -> AsyncGenerator[Tuple[str, Dict[str, Any]], None]:
    """Create licensed dataset."""

    # Create dataset
    dataset_name: str = "wdpa_licensed_protected_areas"

    await async_client.put(
        f"/dataset/{dataset_name}", json={"metadata": DATASET_METADATA}
    )

    # Yield dataset name and associated metadata
    yield dataset_name, DATASET_METADATA

    # Clean up
    await async_client.delete(f"/dataset/{dataset_name}")

@pytest_asyncio.fixture
async def licensed_version(
    async_client: AsyncClient,
    licensed_dataset: Tuple[str, str],
    monkeypatch: MonkeyPatch,
) -> AsyncGenerator[Tuple[str, str, Dict[str, Any]], None]:
    """Create licensed version."""

    dataset_name, _ = licensed_dataset
    version_name: str = "v1"

    await create_vector_source_version(async_client, dataset_name, version_name, monkeypatch)

    # yield version
    yield dataset_name, version_name, VERSION_METADATA

    # clean up
    await async_client.delete(f"/dataset/{dataset_name}/{version_name}")

@pytest_asyncio.fixture
async def apikey(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
) -> AsyncGenerator[Tuple[str, Dict[str, Any]], None]:

    monkeypatch.setattr(api_keys, "add_api_key_to_gateway", void_coroutine)
    monkeypatch.setattr(api_keys, "delete_api_key_from_gateway", void_coroutine)
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


@pytest_asyncio.fixture
async def apikey_unrestricted(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
) -> AsyncGenerator[Tuple[str, Dict[str, Any]], None]:

    monkeypatch.setattr(api_keys, "add_api_key_to_gateway", void_coroutine)
    monkeypatch.setattr(api_keys, "delete_api_key_from_gateway", void_coroutine)
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


@pytest.fixture()
def geojson_bad():
    return _load_geojson("test_bad")


def _load_geojson(name):
    with open(f"{os.path.dirname(__file__)}/fixtures/geojson/{name}.geojson") as src:
        geojson = json.load(src)

    return geojson


@pytest_asyncio.fixture
async def geostore_huge(
    async_client: AsyncClient, geojson_huge
) -> AsyncGenerator[str, None]:
    # Get geostore ID
    geostore_id = await _create_geostore(geojson_huge, async_client)

    yield geostore_id

    # Clean up
    # Nothing to do here. No clean up function for geostore_huge.


@pytest_asyncio.fixture
async def geostore_bad(
    async_client: AsyncClient, geojson_bad
) -> AsyncGenerator[str, None]:
    # Get geostore ID
    geostore_id = await _create_geostore(geojson_bad, async_client)

    yield geostore_id

    # Clean up
    # Nothing to do here. No clean up function for geostore_bad.


@pytest_asyncio.fixture
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

    response = await async_client.post("/geostore", json=payload, follow_redirects=True)
    assert response.status_code == 201

    return response.json()["data"]["gfw_geostore_id"]
