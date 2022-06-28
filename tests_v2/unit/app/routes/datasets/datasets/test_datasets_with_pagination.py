from typing import Tuple

import pytest as pytest
from httpx import AsyncClient

from app.models.pydantic.datasets import PaginatedDatasetsResponse


@pytest.mark.asyncio
async def test_adding_page_number_returns_paginated_datasets_response(
    async_client: AsyncClient, generic_dataset: Tuple[str, str]
) -> None:

    resp = await async_client.get("/datasets?page[number]=1")
    assert PaginatedDatasetsResponse(**resp.json())


@pytest.mark.asyncio
async def test_adding_size_parameter_returns_paginated_datasets_response(
    async_client: AsyncClient, generic_dataset: Tuple[str, str]
) -> None:

    resp = await async_client.get("/datasets?page[size]=10")
    assert PaginatedDatasetsResponse(**resp.json())


@pytest.mark.asyncio
async def test_adding_both_page_and_size_parameter_returns_paginated_datasets_response(
    async_client: AsyncClient, generic_dataset: Tuple[str, str]
) -> None:

    resp = await async_client.get("/datasets?page[number]=1&page[size]=10")
    assert PaginatedDatasetsResponse(**resp.json())
