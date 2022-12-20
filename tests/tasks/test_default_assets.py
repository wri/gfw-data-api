import pytest as pytest
from httpx import AsyncClient

from .. import BUCKET, SHP_NAME
from ..utils import create_default_asset


@pytest.mark.asyncio
async def test_default_asset_cant_delete(batch_client, async_client: AsyncClient):
    _, logs = batch_client

    dataset = "test"

    version = "v1.1.1"
    input_data = {
        "creation_options": {
            "source_type": "vector",
            "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
            "source_driver": "ESRI Shapefile",
            "create_dynamic_vector_tile_cache": False,
        },
    }

    asset = await create_default_asset(
        dataset,
        version,
        version_payload=input_data,
        async_client=async_client,
        logs=logs,
        execute_batch_jobs=False,
        skip_dataset=False,
    )
    asset_id = asset["asset_id"]

    response = await async_client.delete(f"/asset/{asset_id}")
    assert response.status_code == 409
    expected_message = (
        "Deletion failed. You cannot delete a default asset. "
        "To delete a default asset you must delete the parent version."
    )
    assert response.json()["message"] == expected_message
