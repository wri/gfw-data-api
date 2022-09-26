import pytest
from httpx import AsyncClient

from tests_v2.fixtures.metadata.version import VERSION_METADATA
from tests_v2.unit.app.routes.utils import assert_jsend


@pytest.mark.asyncio
async def test_get_version(async_client: AsyncClient, generic_vector_source_version):
    dataset_name, version_name, _ = generic_vector_source_version
    resp = await async_client.get(f"/dataset/{dataset_name}/{version_name}")
    assert resp.status_code == 200
    data = resp.json()
    assert_jsend(data)


@pytest.mark.asyncio
async def test_get_version_metadata(
    async_client: AsyncClient, generic_vector_source_version
):
    dataset_name, version_name, _ = generic_vector_source_version
    resp = await async_client.get(f"/dataset/{dataset_name}/{version_name}/metadata")
    assert resp.status_code == 200
    assert_jsend(resp.json())
    assert (
        resp.json()["data"]["content_date_range"]
        == VERSION_METADATA["content_date_range"]
    )


@pytest.mark.asyncio
async def test_update_version_metadata(
    async_client: AsyncClient, generic_vector_source_version
):
    dataset_name, version_name, _ = generic_vector_source_version
    new_start_date = "2000-02-01"
    new_end_date = "2001-01-03"
    resp = await async_client.patch(
        f"/dataset/{dataset_name}/{version_name}/metadata",
        json={
            "content_date_range": {
                "start_date": new_start_date,
                "end_date": new_end_date,
            }
        },
    )
    assert resp.status_code == 200
    assert_jsend(resp.json())
    assert resp.json()["data"]["content_date_range"]["start_date"] == new_start_date


@pytest.mark.asyncio
async def test_delete_version_metadata(
    async_client: AsyncClient, generic_vector_source_version
):
    dataset_name, version_name, _ = generic_vector_source_version
    resp = await async_client.delete(
        f"/dataset/{dataset_name}/{version_name}/metadata",
    )

    assert resp.status_code == 200

    resp = await async_client.get(
        f"/dataset/{dataset_name}/{version_name}/metadata",
    )
    assert resp.status_code == 404
