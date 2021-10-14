from typing import Any, Dict, Tuple

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_get_alias(
    async_client: AsyncClient, version_alias: Tuple[str, str, str]
) -> None:
    dataset, version, alias = version_alias

    response = await async_client.get(f"/alias/version/{dataset}/{alias}")

    assert response.status_code == 200
    assert response.json()["data"]["version"] == version


@pytest.mark.asyncio
async def test_create_alias(
    async_client: AsyncClient,
    generic_vector_source_version: Tuple[str, str, Dict[str, Any]],
) -> None:
    dataset, version, _ = generic_vector_source_version
    alias = "v202103"

    response = await async_client.put(
        f"/alias/version/{dataset}/{alias}", json={"version": version}
    )

    assert response.status_code == 200
    assert response.json()["data"]["version"] == version
    assert response.json()["data"]["alias"] == alias


@pytest.mark.asyncio
async def test_delete_alias(
    async_client: AsyncClient,
    generic_vector_source_version: Tuple[str, str, Dict[str, Any]],
) -> None:
    dataset, version, _ = generic_vector_source_version
    alias = "v202103"

    await async_client.put(
        f"/alias/version/{dataset}/{alias}", json={"version": version}
    )

    response = await async_client.delete(f"/alias/version/{dataset}/{alias}")

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_version_by_alias(
    async_client: AsyncClient, version_alias: Tuple[str, str, str]
) -> None:
    dataset_name, version_name, alias = version_alias
    response = await async_client.get(f"/dataset/{dataset_name}/{alias}")

    assert response.json()["data"]["version"] == version_name
