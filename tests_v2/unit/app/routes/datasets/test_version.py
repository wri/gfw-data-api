import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient

from app.routes.datasets import versions
from app.tasks import batch
from tests_v2.fixtures.creation_options.versions import VECTOR_SOURCE_CREATION_OPTIONS
from tests_v2.fixtures.metadata.version import VERSION_METADATA
from tests_v2.unit.app.routes.utils import assert_jsend
from tests_v2.utils import BatchJobMock, void_function


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


@pytest.mark.asyncio
async def test_create_version_bare_minimum(
    async_client: AsyncClient, generic_dataset, monkeypatch: MonkeyPatch
):
    version_name = "v42"
    dataset_name, _ = generic_dataset

    # patch all functions which reach out to external services
    batch_job_mock = BatchJobMock()
    monkeypatch.setattr(batch, "submit_batch_job", batch_job_mock.submit_batch_job)
    monkeypatch.setattr(versions, "_verify_source_file_access", void_function)

    payload = {"creation_options": VECTOR_SOURCE_CREATION_OPTIONS}

    resp = await async_client.put(
        f"/dataset/{dataset_name}/{version_name}", json=payload
    )
    assert resp.status_code == 202
    data = resp.json()
    assert_jsend(data)


@pytest.mark.asyncio
async def test_append_version_bare_minimum(
    async_client: AsyncClient, generic_vector_source_version, monkeypatch: MonkeyPatch
):
    dataset_name, version_name, _ = generic_vector_source_version

    # patch all functions which reach out to external services
    batch_job_mock = BatchJobMock()
    monkeypatch.setattr(batch, "submit_batch_job", batch_job_mock.submit_batch_job)
    monkeypatch.setattr(versions, "_verify_source_file_access", void_function)

    payload = {"source_uri": ["s3://some_bucket/test.shp.zip"]}

    resp = await async_client.post(
        f"/dataset/{dataset_name}/{version_name}/append", json=payload
    )
    assert resp.status_code == 200
    data = resp.json()
    assert_jsend(data)
