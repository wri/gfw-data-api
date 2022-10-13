from typing import Any, Dict, Tuple

import pytest as pytest
from httpx import AsyncClient

from app.models.pydantic.tasks import PaginatedTasksResponse


def assets_for(dataset):
    dataset_name, dataset_version, _ = dataset
    return f"/dataset/{dataset_name}/{dataset_version}/assets"


def tasks_for(asset_id):
    return f"/asset/{asset_id}/tasks"


@pytest.mark.asyncio
async def test_adding_page_number_returns_paginated_tasks_response(
    async_client: AsyncClient,
    generic_vector_source_version: Tuple[str, str, Dict[str, Any]],
) -> None:

    assets = await async_client.get(assets_for(generic_vector_source_version))
    assets = assets.json()
    resp = await async_client.get(
        tasks_for(assets["data"][0]["asset_id"]), params=[("page[number]", "1")]
    )
    assert PaginatedTasksResponse(**resp.json())


@pytest.mark.asyncio
async def test_adding_size_parameter_returns_paginated_tasks_response(
    async_client: AsyncClient,
    generic_vector_source_version: Tuple[str, str, Dict[str, Any]],
) -> None:

    assets = await async_client.get(assets_for(generic_vector_source_version))
    assets = assets.json()
    resp = await async_client.get(
        tasks_for(assets["data"][0]["asset_id"]), params=[("page[size]", "10")]
    )
    assert PaginatedTasksResponse(**resp.json())


@pytest.mark.asyncio
async def test_adding_both_page_and_size_parameter_returns_paginated_assets_response(
    async_client: AsyncClient,
    generic_vector_source_version: Tuple[str, str, Dict[str, Any]],
) -> None:

    assets = await async_client.get(assets_for(generic_vector_source_version))
    assets = assets.json()
    resp = await async_client.get(
        tasks_for(assets["data"][0]["asset_id"]),
        params=[("page[number]", "1"), ("page[size]", "10")],
    )
    assert PaginatedTasksResponse(**resp.json())


@pytest.mark.asyncio
async def test_get_paginated_asset_with_pagesize_less_than_1_returns_4xx(
    async_client: AsyncClient,
    generic_vector_source_version: Tuple[str, str, Dict[str, Any]],
) -> None:

    assets = await async_client.get(assets_for(generic_vector_source_version))
    assets = assets.json()
    resp = await async_client.get(
        tasks_for(assets["data"][0]["asset_id"]), params=[("page[size]", "0")]
    )
    print(resp.json())
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_paginated_asset_with_pagenumber_less_than_1_returns_4xx(
    async_client: AsyncClient,
    generic_vector_source_version: Tuple[str, str, Dict[str, Any]],
) -> None:

    assets = await async_client.get(assets_for(generic_vector_source_version))
    assets = assets.json()
    resp = await async_client.get(
        tasks_for(assets["data"][0]["asset_id"]), params=[("page[number]", "0")]
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_paginated_asset_with_pagenumber_more_than_max_pages_returns_4xx(
    async_client: AsyncClient,
    generic_vector_source_version: Tuple[str, str, Dict[str, Any]],
) -> None:

    assets = await async_client.get(assets_for(generic_vector_source_version))
    assets = assets.json()
    resp = await async_client.get(
        tasks_for(assets["data"][0]["asset_id"]), params=[("page[number]", "100")]
    )
    assert resp.status_code == 422
