from typing import Any, Dict

from httpx import AsyncClient

from app.application import app
from tests import BUCKET, SHP_NAME

generic_dataset_metadata = {
    "metadata": {
        "title": "string",
        "subtitle": "string",
        "function": "string",
        "resolution": "string",
        "geographic_coverage": "string",
        "source": "string",
        "update_frequency": "string",
        "cautions": "string",
        "license": "string",
        "overview": "string",
        "citation": "string",
        "tags": ["string"],
        "data_language": "string",
        "key_restrictions": "string",
        "scale": "string",
        "added_date": "2020-06-25",
        "why_added": "string",
        "other": "string",
        "learn_more": "string",
    }
}

generic_version_metadata = {
    "is_latest": True,
    "source_type": "vector",
    "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
    "metadata": {},
    "creation_options": {"src_driver": "ESRI Shapefile", "zipped": True},
}


async def create_dataset(dataset_name, metadata: Dict[str, Any]) -> Dict[str, Any]:
    async with AsyncClient(
        app=app, base_url="http://test", trust_env=False
    ) as test_client:
        resp = await test_client.put(f"/meta/{dataset_name}", json=metadata)
    assert resp.json()["status"] == "success"
    return resp.json()["data"]


async def create_version(
    dataset_name, version, version_metadata: Dict[str, Any]
) -> Dict[str, Any]:
    async with AsyncClient(
        app=app, base_url="http://test", trust_env=False
    ) as test_client:
        resp = await test_client.put(
            f"/meta/{dataset_name}/{version}", json=version_metadata
        )
    assert resp.json()["status"] == "success"

    return resp.json()["data"]


async def create_default_asset(
    dataset_name,
    version,
    dataset_metadata: Dict[str, Any] = generic_dataset_metadata,
    version_metadata: Dict[str, Any] = generic_version_metadata,
) -> Dict[str, Any]:
    # Create dataset, version, and default asset records.
    # The default asset is created automatically when the version is created.

    _ = await create_dataset(dataset_name, dataset_metadata)
    _ = await create_version(dataset_name, version, version_metadata)

    # Verify the default asset was created
    async with AsyncClient(
        app=app, base_url="http://test", trust_env=False
    ) as test_client:
        resp = await test_client.get(f"/meta/{dataset_name}/{version}/assets")

    assert len(resp.json()["data"]) == 1
    assert resp.json()["status"] == "success"

    return resp.json()["data"][0]
