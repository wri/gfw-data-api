import pytest as pytest
from httpx import AsyncClient

from app.models.pydantic.datasets import PaginatedDatasetsResponse


@pytest.mark.asyncio
async def test_adding_page_number_returns_paginated_datasets_response(
    async_client: AsyncClient,
) -> None:

    resp = await async_client.get("/datasets", params=[("page[number]", "1")])
    assert PaginatedDatasetsResponse(**resp.json())


@pytest.mark.asyncio
async def test_adding_size_parameter_returns_paginated_datasets_response(
    async_client: AsyncClient,
) -> None:

    resp = await async_client.get("/datasets", params=[("page[size]", "10")])
    assert PaginatedDatasetsResponse(**resp.json())


@pytest.mark.asyncio
async def test_adding_both_page_and_size_parameter_returns_paginated_datasets_response(
    async_client: AsyncClient,
) -> None:

    resp = await async_client.get(
        "/datasets", params=[("page[number]", "1"), ("page[size]", "10")]
    )
    assert PaginatedDatasetsResponse(**resp.json())


@pytest.mark.asyncio
async def test_get_paginated_dataset_with_pagesize_less_than_1_returns_4xx(
    async_client: AsyncClient,
) -> None:
    resp = await async_client.get("/datasets", params=[("page[size]", "0")])
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_paginated_dataset_with_pagenumber_less_than_1_returns_4xx(
    async_client: AsyncClient,
) -> None:
    resp = await async_client.get("/datasets", params=[("page[number]", "0")])
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_paginated_dataset_with_pagenumber_more_than_max_pages_returns_4xx(
    async_client: AsyncClient,
) -> None:
    resp = await async_client.get("/datasets", params=[("page[number]", "100")])
    assert resp.status_code == 422
