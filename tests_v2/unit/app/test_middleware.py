import pytest
from httpx import AsyncClient, Response


@pytest.mark.asyncio
async def test_redirect_latest(
    async_client: AsyncClient, generic_vector_source_version
):
    """Test if middleware redirects to correct version when using `latest`
    version identifier."""

    dataset, version, _ = generic_vector_source_version

    response: Response = await async_client.get(f"/dataset/{dataset}/{version}")
    assert response.status_code == 200

    response = await async_client.get(f"/dataset/{dataset}/latest")
    assert response.status_code == 404

    # Promote current version to `latest`
    response = await async_client.patch(
        f"/dataset/{dataset}/{version}", json={"is_latest": True}
    )
    assert response.status_code == 200
    assert response.json()["data"]["is_latest"] is True

    # Now the latest redirect should work
    response = await async_client.get(
        f"/dataset/{dataset}/latest", follow_redirects=False
    )
    assert response.status_code == 307
    assert response.headers["location"] == f"/dataset/{dataset}/{version}"

    params = {"test": "query"}
    response = await async_client.get(
        f"/dataset/{dataset}/latest", params=params, follow_redirects=False
    )
    assert response.status_code == 307
    assert response.headers["location"] == f"/dataset/{dataset}/{version}?test=query"

    response = await async_client.post(
        f"/dataset/{dataset}/latest/query/json", follow_redirects=False
    )
    assert response.status_code == 307
    assert response.headers["location"] == f"/dataset/{dataset}/{version}/query/json"

    response = await async_client.post(
        f"/dataset/{dataset}/latest/download/csv", follow_redirects=False
    )
    assert response.status_code == 307
    assert response.headers["location"] == f"/dataset/{dataset}/{version}/download/csv"

    # except for POST requests which do not point to query or download
    response = await async_client.post(
        f"/dataset/{dataset}/latest/assets", follow_redirects=False
    )
    assert response.status_code == 400
