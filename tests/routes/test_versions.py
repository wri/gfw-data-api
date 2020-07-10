import json
from unittest.mock import patch

import pytest

from app.models.pydantic.metadata import VersionMetadata
from tests import BUCKET, SHP_NAME

payload = {
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


# @patch("app.tasks.default_assets.create_default_asset", return_value=True)
@patch("fastapi.BackgroundTasks.add_task", return_value=None)
@pytest.mark.asyncio
async def test_versions(mocked_task, async_client):
    """Test version path operations.

    We patch/ disable background tasks here, as they run asynchronously.
    Such tasks are tested separately in a different module
    """
    dataset = "test"
    version = "v1.1.1"

    response = await async_client.put(f"/dataset/{dataset}", data=json.dumps(payload))
    assert response.status_code == 201
    assert response.json()["data"]["metadata"] == payload["metadata"]
    assert response.json()["data"]["versions"] == []

    version_payload = {
        "is_latest": True,
        "creation_options": {
            "source_type": "vector",
            "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
            "metadata": payload["metadata"],
            "source_driver": "ESRI Shapefile",
            "zipped": True,
        },
        "metadata": {},
    }

    with patch("app.tasks.default_assets.create_default_asset", return_value=True):
        response = await async_client.put(
            f"/dataset/{dataset}/{version}", data=json.dumps(version_payload)
        )
    print(response.json())
    version_data = response.json()
    assert response.status_code == 202
    assert version_data["data"]["dataset"] == dataset
    assert version_data["data"]["version"] == version
    assert version_data["data"]["metadata"] == VersionMetadata(**payload["metadata"])
    assert mocked_task.called
    response = await async_client.get(f"/dataset/{dataset}/{version}")
    assert response.json()["data"]["version"] == "v1.1.1"

    response = await async_client.patch(
        f"/dataset/{dataset}/{version}", json={"is_latest": True}
    )
    assert response.status_code == 200

    # Check if the latest endpoint redirects us to v1.1.1
    response = await async_client.get(
        f"/dataset/{dataset}/latest?test=test&test1=test1"
    )
    assert response.json()["data"]["version"] == "v1.1.1"


@pytest.mark.asyncio
@patch("fastapi.BackgroundTasks.add_task", return_value=None)
async def test_version_metadata(mocked_task, async_client):
    """Test if Version inherits metadata from Dataset.

    Version should be able to overwrite any metadata attribute
    """
    dataset = "test"
    version = "v1.1.1"

    dataset_metadata = {"title": "Title", "subtitle": "Subtitle"}

    response = await async_client.put(
        f"/dataset/{dataset}", data=json.dumps({"metadata": dataset_metadata})
    )

    result_metadata = {
        "title": "Title",
        "subtitle": "Subtitle",
        "function": None,
        "resolution": None,
        "geographic_coverage": None,
        "source": None,
        "update_frequency": None,
        "cautions": None,
        "license": None,
        "overview": None,
        "citation": None,
        "tags": None,
        "data_language": None,
        "key_restrictions": None,
        "scale": None,
        "added_date": None,
        "why_added": None,
        "other": None,
        "learn_more": None,
    }

    assert response.status_code == 201
    assert response.json()["data"]["metadata"] == result_metadata

    version_metadata = {"subtitle": "New Subtitle", "version_number": version}

    version_payload = {
        "metadata": version_metadata,
        "creation_options": {
            "source_type": "vector",
            "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
            "source_driver": "ESRI Shapefile",
            "zipped": True,
        },
    }

    response = await async_client.put(
        f"/dataset/{dataset}/{version}", data=json.dumps(version_payload)
    )

    result_metadata = {
        "title": "Title",
        "subtitle": "New Subtitle",
        "function": None,
        "resolution": None,
        "geographic_coverage": None,
        "source": None,
        "update_frequency": None,
        "cautions": None,
        "license": None,
        "overview": None,
        "citation": None,
        "tags": None,
        "data_language": None,
        "key_restrictions": None,
        "scale": None,
        "added_date": None,
        "why_added": None,
        "other": None,
        "learn_more": None,
        "version_number": version,
        "content_date": None,
        "last_update": None,
        "download": None,
        "analysis": None,
        "data_updates": None,
    }

    assert response.status_code == 202
    assert response.json()["data"]["metadata"] == result_metadata
    assert mocked_task.called

    response = await async_client.get(f"/dataset/{dataset}/{version}")
    assert response.json()["data"]["metadata"] == result_metadata

    response = await async_client.delete(f"/dataset/{dataset}/{version}")
    assert response.json()["data"]["metadata"] == result_metadata


@pytest.mark.asyncio
@patch("fastapi.BackgroundTasks.add_task", return_value=None)
async def test_version_delete_protection(mocked_task, async_client):
    dataset = "test"
    version1 = "v1.1.1"
    version2 = "v1.1.2"

    await async_client.put(f"/dataset/{dataset}", data=json.dumps(payload))

    version_payload = {
        "metadata": payload["metadata"],
        "creation_options": {
            "source_type": "vector",
            "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
            "source_driver": "ESRI Shapefile",
            "zipped": True,
        },
    }

    response = await async_client.put(
        f"/dataset/{dataset}/{version1}", json=version_payload
    )
    assert response.status_code == 202

    response = await async_client.put(
        f"/dataset/{dataset}/{version2}", json=version_payload
    )
    assert response.status_code == 202

    response = await async_client.patch(
        f"/dataset/{dataset}/{version2}", json={"is_latest": True}
    )
    assert response.status_code == 200

    response = await async_client.delete(f"/dataset/{dataset}/{version2}")

    assert response.status_code == 409

    await async_client.delete(f"/dataset/{dataset}/{version1}")
    response = await async_client.delete(f"/dataset/{dataset}/{version2}")
    assert response.status_code == 200
    assert mocked_task.called


@pytest.mark.asyncio
@patch("fastapi.BackgroundTasks.add_task", return_value=None)
async def test_latest_middleware(mocked_task, async_client):
    """Test if middleware redirects to correct version when using `latest`
    version identifier."""

    dataset = "test"
    version = "v1.1.1"

    response = await async_client.put(f"/dataset/{dataset}", data=json.dumps(payload))
    print(response.json())
    assert response.status_code == 201

    version_payload = {
        "metadata": payload["metadata"],
        "creation_options": {
            "source_type": "vector",
            "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
            "source_driver": "ESRI Shapefile",
            "zipped": True,
        },
    }

    response = await async_client.put(
        f"/dataset/{dataset}/{version}", json=version_payload
    )
    print(response.json())
    assert response.status_code == 202

    response = await async_client.get(f"/dataset/{dataset}/{version}")
    print(response.json())
    assert response.status_code == 200

    response = await async_client.get(f"/dataset/{dataset}/latest")
    print(response.json())
    assert response.status_code == 404

    response = await async_client.patch(
        f"/dataset/{dataset}/{version}", data=json.dumps({"is_latest": True})
    )
    print(response.json())
    assert response.status_code == 200
    assert response.json()["data"]["is_latest"] is True

    response = await async_client.get(f"/dataset/{dataset}/latest")
    print(response.json())
    assert response.status_code == 200
    assert response.json()["data"]["version"] == version
