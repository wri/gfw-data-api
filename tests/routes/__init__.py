import json
import uuid
from typing import Any, Dict

from tests import BUCKET, SHP_NAME

generic_dataset_payload = {
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

generic_version_payload = {
    "metadata": {},
    "creation_options": {
        "source_driver": "ESRI Shapefile",
        "zipped": True,
        "source_type": "vector",
        "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
    },
}


async def create_dataset(
    async_client, dataset_name, payload: Dict[str, Any]
) -> Dict[str, Any]:
    resp = await async_client.put(f"/dataset/{dataset_name}", data=json.dumps(payload))
    assert resp.json()["status"] == "success"
    return resp.json()["data"]


async def create_version(
    async_client, dataset_name, version, payload: Dict[str, Any]
) -> Dict[str, Any]:

    resp = await async_client.put(
        f"/dataset/{dataset_name}/{version}", data=json.dumps(payload)
    )
    assert resp.json()["status"] == "success"

    return resp.json()["data"]


async def create_default_asset(
    async_client,
    dataset_name,
    version,
    dataset_payload: Dict[str, Any] = generic_dataset_payload,
    version_payload: Dict[str, Any] = generic_version_payload,
) -> Dict[str, Any]:
    # Create dataset, version, and default asset records.
    # The default asset is created automatically when the version is created.

    await create_dataset(async_client, dataset_name, dataset_payload)
    await create_version(async_client, dataset_name, version, version_payload)

    # Verify the default asset was created
    resp = await async_client.get(f"/dataset/{dataset_name}/{version}/assets")
    print(resp.json())
    assert len(resp.json()["data"]) == 1
    assert resp.json()["status"] == "success"

    return resp.json()["data"][0]


def generate_uuid(*args, **kwargs) -> uuid.UUID:
    return uuid.uuid4()
