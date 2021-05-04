from typing import Any, AsyncGenerator, Dict, Generator, Tuple

import pytest
from _pytest.monkeypatch import MonkeyPatch
from alembic.config import main
from fastapi.testclient import TestClient
from httpx import AsyncClient
from requests import Session

from app.authentication.token import get_user_id, is_admin, is_service_account
from app.routes.datasets import versions
from app.tasks import batch, delete_assets, vector_source_assets
from tests_v2.fixtures.creation_options.versions import VECTOR_SOURCE_CREATION_OPTIONS
from tests_v2.utils import (
    _create_vector_source_assets,
    dict_function_closure,
    false_function,
    generate_uuid,
    get_user_id_mocked,
    int_function_closure,
    is_admin_mocked,
    is_service_account_mocked,
    void_function,
)


@pytest.fixture()
def db():
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
    """We need to inialize the database before running test.

    We can do that using the Test Client
    """
    from app.main import app

    with TestClient(app):
        yield


@pytest.fixture()
def client(db) -> Generator[Session, None, None]:
    """Synchronous Test Client."""

    from app.main import app

    app.dependency_overrides[is_admin] = is_admin_mocked
    app.dependency_overrides[is_service_account] = is_service_account_mocked
    app.dependency_overrides[get_user_id] = get_user_id_mocked
    # app.dependency_overrides[get_api_key] = get_api_key_mocked

    with TestClient(app) as client:
        yield client

    app.dependency_overrides = {}


@pytest.fixture()
@pytest.mark.asyncio
async def async_client(db) -> AsyncGenerator[AsyncClient, None]:
    """Async Test Client."""
    from app.main import app

    app.dependency_overrides[is_admin] = is_admin_mocked
    app.dependency_overrides[is_service_account] = is_service_account_mocked
    app.dependency_overrides[get_user_id] = get_user_id_mocked
    # app.dependency_overrides[get_api_key] = get_api_key_mocked

    async with AsyncClient(app=app, base_url="http://test", trust_env=False) as client:
        yield client

    app.dependency_overrides = {}


@pytest.fixture()
def generic_dataset(
    client: Session,
) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
    """Create generic dataset."""

    dataset_name: str = "my_first_dataset"
    dataset_metadata: Dict[str, Any] = {}

    client.put(f"/dataset/{dataset_name}", json={"metadata": dataset_metadata})
    yield dataset_name, dataset_metadata

    client.delete(f"/dataset/{dataset_name}")


@pytest.fixture()
def generic_vector_source_version(
    client: Session, generic_dataset: Tuple[str, str]
) -> Generator[Tuple[str, str, Dict[str, Any]], None, None]:
    """Create generic vector source version."""

    monkeypatch_version()

    dataset_name, _ = generic_dataset
    version_name: str = "v1"
    version_metadata: Dict[str, Any] = {}

    creation_options = VECTOR_SOURCE_CREATION_OPTIONS

    # Create version
    client.put(
        f"/dataset/{generic_dataset}/{version_name}",
        json={
            "metadata": version_metadata,
            "creation_options": creation_options,
        },
    )

    # mock batch processes
    # TODO: create vector layer

    # yield version
    yield dataset_name, version_name, version_metadata

    # clean up
    client.delete(f"/dataset/{dataset_name}/{version_name}")


@pytest.fixture()
@pytest.mark.asyncio()
async def generic_vector_source_version_async(
    async_client: AsyncClient, generic_dataset: Tuple[str, str], monkeypatch_version
) -> AsyncGenerator[Tuple[str, str, Dict[str, Any]], None]:
    """Create generic vector source version."""

    dataset_name, _ = generic_dataset
    version_name: str = "v1"
    version_metadata: Dict[str, Any] = {}

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

    # yield version
    yield dataset_name, version_name, version_metadata

    # clean up
    await async_client.delete(f"/dataset/{dataset_name}/{version_name}")


@pytest.fixture()
@pytest.mark.asyncio()
async def apikey(async_client: AsyncClient):
    payload = {
        "alias": "test",
        "organization": "Global Forest Watch",
        "email": "admin@globalforestwatch.org",
        "domains": ["*.globalforestwatch.org"],
    }

    response = await async_client.post("/auth/apikey", json=payload)
    print(response.json())
    api_key = response.json()["data"]["api_key"]
    origin = "www.globalforestwatch.org"

    yield api_key, origin

    await async_client.delete(f"auth/apikey/{api_key}")


@pytest.fixture()
def monkeypatch_version(monkeypatch: MonkeyPatch):
    # patch all functions which reach out to external services
    monkeypatch.setattr(versions, "_verify_source_file_access", void_function)
    monkeypatch.setattr(batch, "submit_batch_job", generate_uuid)
    monkeypatch.setattr(vector_source_assets, "is_zipped", false_function)
    monkeypatch.setattr(delete_assets, "delete_s3_objects", int_function_closure(1))
    monkeypatch.setattr(
        delete_assets, "flush_cloudfront_cache", dict_function_closure({})
    )
    yield
