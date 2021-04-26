from typing import Any, AsyncGenerator, Dict, Generator, Tuple

import pytest
from _pytest.monkeypatch import MonkeyPatch
from alembic.config import main
from fastapi.testclient import TestClient
from httpx import AsyncClient
from requests import Session

from app.authentication.api_keys import get_api_key
from app.authentication.token import get_user_id, is_admin, is_service_account
from app.routes.datasets import versions
from app.tasks import batch, delete_assets, vector_source_assets
from tests_v2.utils import (
    dict_function_closure,
    false_function,
    generate_uuid,
    get_api_key_mocked,
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


@pytest.fixture()
def client(db) -> Generator[Session, None, None]:
    """Synchronous Test Client."""

    from app.main import app

    app.dependency_overrides[is_admin] = is_admin_mocked
    app.dependency_overrides[is_service_account] = is_service_account_mocked
    app.dependency_overrides[get_user_id] = get_user_id_mocked
    app.dependency_overrides[get_api_key] = get_api_key_mocked

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
    app.dependency_overrides[get_api_key] = get_api_key_mocked

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
    client: Session, generic_dataset: Tuple[str, str], monkeypatch: MonkeyPatch
) -> Generator[Tuple[str, str, Dict[str, Any]], None, None]:
    """Create generic vector source version."""
    # patch all functions which reach out to external services
    monkeypatch.setattr(versions, "_verify_source_file_access", void_function)
    monkeypatch.setattr(batch, "submit_batch_job", generate_uuid)
    monkeypatch.setattr(vector_source_assets, "is_zipped", false_function)
    monkeypatch.setattr(delete_assets, "delete_s3_objects", int_function_closure(1))
    monkeypatch.setattr(
        delete_assets, "flush_cloudfront_cache", dict_function_closure({})
    )

    dataset_name, _ = generic_dataset
    version_name: str = "v1"
    version_metadata: Dict[str, Any] = {}
    bucket = "my_bucket"
    shp_name = "my_shape.zip"
    vector_source_creation_options = {
        "source_driver": "ESRI Shapefile",
        "source_type": "vector",
        "source_uri": [f"s3://{bucket}/{shp_name}"],
        "layers": None,
        "indices": [
            {"column_names": ["geom"], "index_type": "gist"},
            {"column_names": ["geom_wm"], "index_type": "gist"},
            {"column_names": ["gfw_geostore_id"], "index_type": "hash"},
        ],
        "create_dynamic_vector_tile_cache": True,
        "add_to_geostore": True,
    }

    client.put(
        f"/dataset/{dataset_name}/{version_name}",
        json={
            "metadata": version_metadata,
            "creation_options": vector_source_creation_options,
        },
    )

    yield dataset_name, version_name, version_metadata

    client.delete(f"/dataset/{dataset_name}/{version_name}")
