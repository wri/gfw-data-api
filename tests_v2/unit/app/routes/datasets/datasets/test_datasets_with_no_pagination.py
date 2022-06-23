from typing import Tuple

import pytest as pytest
from httpx import AsyncClient

from app.models.pydantic.datasets import DatasetsResponse


@pytest.mark.asyncio
async def test_get_datasets_returns_datasets_response(
    async_client: AsyncClient, generic_dataset: Tuple[str, str]
) -> None:

    resp = await async_client.get("/datasets")
    assert DatasetsResponse(**resp.json())
