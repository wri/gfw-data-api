from typing import Tuple

import pytest
from fastapi.exceptions import HTTPException
from httpx import AsyncClient

from app.authentication.token import get_manager
from app.models.pydantic.datasets import DatasetResponse
from app.routes.datasets.dataset import get_owner
from tests_v2.fixtures.metadata.dataset import DATASET_METADATA
from tests_v2.unit.app.routes.utils import assert_jsend
from tests_v2.utils import (
    get_admin_mocked,
    get_manager_mocked,
    get_user_mocked,
    raises_401,
)


@pytest.mark.asyncio
async def test_get_owner_fail(db, init_db, monkeypatch) -> None:
    dataset_name: str = "my_first_dataset"

    from app.main import app

    # Create a dataset
    app.dependency_overrides[get_manager] = get_manager_mocked

    async with AsyncClient(
        app=app,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as async_client:
        create_resp = await async_client.put(
            f"/dataset/{dataset_name}", json={"metadata": {}}
        )
        assert create_resp.status_code == 201

    app.dependency_overrides = {}

    some_user = await get_user_mocked()

    try:
        _ = await get_owner(dataset_name, some_user)
    except HTTPException as e:
        assert e.status_code == 401
        assert e.detail == "Unauthorized"


async def test_get_owner_manager_success(db, init_db, monkeypatch) -> None:
    dataset_name: str = "my_first_dataset"

    from app.main import app

    # Create a dataset
    app.dependency_overrides[get_manager] = get_manager_mocked

    async with AsyncClient(
        app=app,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as async_client:
        create_resp = await async_client.put(
            f"/dataset/{dataset_name}", json={"metadata": {}}
        )
        assert create_resp.status_code == 201

    app.dependency_overrides = {}

    some_manager = await get_manager_mocked()

    _ = await get_owner(dataset_name, some_manager)


async def test_get_owner_different_manager_fail(db, init_db, monkeypatch) -> None:
    dataset_name: str = "my_first_dataset"

    from app.main import app

    # Create a dataset
    app.dependency_overrides[get_manager] = get_manager_mocked

    async with AsyncClient(
        app=app,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as async_client:
        create_resp = await async_client.put(
            f"/dataset/{dataset_name}", json={"metadata": {}}
        )
        assert create_resp.status_code == 201

    app.dependency_overrides = {}

    some_manager = await get_manager_mocked()
    some_manager.id = "Some other manager"

    try:
        _ = await get_owner(dataset_name, some_manager)
    except HTTPException as e:
        assert e.status_code == 401
        assert e.detail == "Unauthorized"


async def test_get_owner_admin_success(db, init_db, monkeypatch) -> None:
    dataset_name: str = "my_first_dataset"

    from app.main import app

    # Create a dataset
    app.dependency_overrides[get_manager] = get_admin_mocked

    async with AsyncClient(
        app=app,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as async_client:
        create_resp = await async_client.put(
            f"/dataset/{dataset_name}", json={"metadata": {}}
        )
        assert create_resp.status_code == 201

    app.dependency_overrides = {}

    some_admin = await get_admin_mocked()

    _ = await get_owner(dataset_name, some_admin)


@pytest.mark.asyncio
async def test_get_dataset_success(
    async_client: AsyncClient, generic_dataset: Tuple[str, str]
) -> None:
    dataset_name, _ = generic_dataset
    resp = await async_client.get(f"/dataset/{dataset_name}")
    assert resp.status_code == 200
    _validate_dataset_response(resp.json(), dataset_name)


@pytest.mark.asyncio
async def test_get_dataset_fail(
    async_client: AsyncClient, generic_dataset: Tuple[str, str]
) -> None:
    dataset_name: str = "not_a_real_dataset"
    resp = await async_client.get(f"/dataset/{dataset_name}")
    assert resp.status_code == 404


# TODO: Use mark.parameterize to test variations
@pytest.mark.asyncio
async def test_create_dataset(async_client: AsyncClient) -> None:
    dataset_name = "my_first_dataset"

    resp = await async_client.put(
        "/dataset/my_first_dataset", json={"metadata": DATASET_METADATA}
    )
    assert resp.status_code == 201
    _validate_dataset_response(resp.json(), dataset_name)


def test_update_dataset():
    pass


@pytest.mark.asyncio
async def test_delete_dataset_requires_creds_fail(db, init_db) -> None:
    dataset_name: str = "my_first_dataset"

    from app.main import app

    # Create a dataset
    app.dependency_overrides[get_manager] = get_manager_mocked

    async with AsyncClient(
        app=app,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as async_client:
        create_resp = await async_client.put(
            f"/dataset/{dataset_name}", json={"metadata": DATASET_METADATA}
        )
        assert create_resp.status_code == 201

    app.dependency_overrides = {}
    app.dependency_overrides[get_owner] = raises_401

    async with AsyncClient(
        app=app,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as async_client:
        delete_resp = await async_client.delete(f"/dataset/{dataset_name}")
        assert delete_resp.json()["message"] == "Unauthorized"
        assert delete_resp.status_code == 401

    app.dependency_overrides = {}


@pytest.mark.asyncio
async def test_delete_dataset_requires_creds_succeed(db, init_db, monkeypatch) -> None:
    dataset_name: str = "my_first_dataset"

    from app.main import app

    # Create a dataset
    app.dependency_overrides[get_manager] = get_manager_mocked

    async with AsyncClient(
        app=app,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as async_client:
        create_resp = await async_client.put(
            f"/dataset/{dataset_name}", json={"metadata": DATASET_METADATA}
        )
        assert create_resp.status_code == 201

    app.dependency_overrides = {}
    app.dependency_overrides[get_owner] = get_manager_mocked

    async with AsyncClient(
        app=app,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as async_client:
        delete_resp = await async_client.delete(f"/dataset/{dataset_name}")
        assert delete_resp.status_code == 200

    app.dependency_overrides = {}


def test__dataset_response():
    pass


def _validate_dataset_response(data, dataset_name: str) -> None:
    assert_jsend(data)
    model = DatasetResponse(**data)

    assert model.data.dataset == dataset_name
    assert model.data.metadata.data_language == DATASET_METADATA["data_language"]
    assert model.data.metadata.source == DATASET_METADATA["source"]
    assert model.data.metadata.title == DATASET_METADATA["title"]
    assert model.data.metadata.overview == DATASET_METADATA["overview"]

    assert model.data.versions == list()
