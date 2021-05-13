import pytest
from httpx import AsyncClient

from tests_v2.unit.routes.utils import assert_jsend


@pytest.mark.asyncio
async def test_get_version(async_client: AsyncClient, generic_vector_source_version):
    dataset_name, version_name, _ = generic_vector_source_version
    resp = await async_client.get(f"/dataset/{dataset_name}/{version_name}")
    assert resp.status_code == 200
    data = resp.json()
    assert_jsend(data)