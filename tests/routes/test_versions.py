import json
from unittest.mock import patch

import pytest

from app.models.pydantic.metadata import VersionMetadata
from tests import BUCKET, SHP_NAME
from tests.tasks import MockCloudfrontClient
from tests.utils import create_dataset, create_default_asset, create_version

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

version_payload = {
    "is_latest": True,
    "creation_options": {
        "source_type": "vector",
        "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
        "source_driver": "ESRI Shapefile",
        "zipped": True,
    },
    "metadata": payload["metadata"],
}


@pytest.mark.asyncio
@patch("app.tasks.aws_tasks.get_cloudfront_client")
async def test_versions(mocked_cloudfront_client, async_client):
    """Test version path operations.

    We patch/ disable background tasks here, as they run asynchronously.
    Such tasks are tested separately in a different module
    """
    dataset = "test"
    version = "v1.1.1"

    mocked_cloudfront_client.return_value = MockCloudfrontClient()

    await create_default_asset(
        dataset,
        version,
        version_payload=version_payload,
        async_client=async_client,
        execute_batch_jobs=False,
    )

    response = await async_client.get(f"/dataset/{dataset}/{version}")
    version_data = response.json()

    assert version_data["data"]["is_latest"] is False
    assert version_data["data"]["dataset"] == dataset
    assert version_data["data"]["version"] == version
    assert version_data["data"]["metadata"] == VersionMetadata(**payload["metadata"])
    assert version_data["data"]["version"] == "v1.1.1"

    ###############
    # Lastest Tag
    ###############

    response = await async_client.patch(
        f"/dataset/{dataset}/{version}", json={"is_latest": True}
    )
    print(response.json())
    assert response.status_code == 200

    # Check if the latest endpoint redirects us to v1.1.1
    response = await async_client.get(
        f"/dataset/{dataset}/latest?test=test&test1=test1"
    )
    assert response.json()["data"]["version"] == "v1.1.1"

    ##################################################
    # additional attributes coming from default asset
    ##################################################

    # Creation Options

    version_creation_options = {
        "source_driver": "ESRI Shapefile",
        "source_type": "vector",
        "source_uri": [f"s3://{BUCKET}/{SHP_NAME}"],
        "layers": None,
        "zipped": True,
        "indices": [
            {"column_name": "geom", "index_type": "gist"},
            {"column_name": "geom_wm", "index_type": "gist"},
            {"column_name": "gfw_geostore_id", "index_type": "hash"},
        ],
        "create_dynamic_vector_tile_cache": True,
    }

    response = await async_client.get(f"/dataset/{dataset}/{version}/creation_options")
    assert response.status_code == 200
    assert response.json()["data"] == version_creation_options

    # Change Log

    response = await async_client.get(f"/dataset/{dataset}/{version}/change_log")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 1

    assert mocked_cloudfront_client.called


@pytest.mark.asyncio
async def test_version_metadata(async_client):
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

    with patch("app.tasks.default_assets.create_default_asset", return_value=True):
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

    response = await async_client.get(f"/dataset/{dataset}/{version}")
    assert response.json()["data"]["metadata"] == result_metadata

    with patch("fastapi.BackgroundTasks.add_task", return_value=None) as mocked_task:
        response = await async_client.delete(f"/dataset/{dataset}/{version}")
        assert response.json()["data"]["metadata"] == result_metadata
        assert mocked_task.called


@pytest.mark.asyncio
@patch("app.tasks.aws_tasks.get_cloudfront_client")
async def test_version_delete_protection(mocked_cloudfront_client, async_client):
    dataset = "test"
    version1 = "v1.1.1"
    version2 = "v1.1.2"

    mocked_cloudfront_client.return_value = MockCloudfrontClient()

    await create_default_asset(dataset, version1, async_client=async_client)
    await create_default_asset(
        dataset, version2, async_client=async_client, skip_dataset=True
    )

    response = await async_client.patch(
        f"/dataset/{dataset}/{version2}", json={"is_latest": True}
    )
    assert response.status_code == 200

    response = await async_client.delete(f"/dataset/{dataset}/{version2}")

    assert response.status_code == 409

    await async_client.delete(f"/dataset/{dataset}/{version1}")
    response = await async_client.delete(f"/dataset/{dataset}/{version2}")
    assert response.status_code == 200
    assert mocked_cloudfront_client.called


@pytest.mark.asyncio
@patch("app.tasks.aws_tasks.get_cloudfront_client")
async def test_latest_middleware(mocked_cloudfront_client, async_client):
    """Test if middleware redirects to correct version when using `latest`
    version identifier."""

    mocked_cloudfront_client.return_value = MockCloudfrontClient()

    dataset = "test"
    version = "v1.1.1"

    await create_default_asset(dataset, version, async_client=async_client)

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

    assert mocked_cloudfront_client.called


@pytest.mark.asyncio
async def test_invalid_source_uri(async_client):
    """Test version path operations.

    We patch/ disable background tasks here, as they run asynchronously.
    Such tasks are tested separately in a different module
    """
    dataset = "test"
    version = "v1.1.1"

    source_uri = [
        "s3://doesnotexist",
        "s3://bucket/key",
        "http://domain/file",
        f"s3://{BUCKET}/{SHP_NAME}",
    ]
    version_payload = {
        "is_latest": True,
        "creation_options": {
            "source_type": "vector",
            "source_uri": source_uri,
            "metadata": payload["metadata"],
            "source_driver": "ESRI Shapefile",
            "zipped": True,
        },
        "metadata": {},
    }

    await create_dataset(dataset, async_client, payload)
    response = await async_client.put(
        f"/dataset/{dataset}/{version}", json=version_payload
    )
    print(response.json())
    assert response.status_code == 400
    assert response.json()["status"] == "failed"
    assert (
        response.json()["message"]
        == "Cannot access source files ['s3://doesnotexist', 's3://bucket/key', 'http://domain/file']"
    )

    response = await async_client.post(
        f"/dataset/{dataset}/{version}/append", json={"source_uri": source_uri}
    )
    assert response.status_code == 400
    assert response.json()["status"] == "failed"
    assert (
        response.json()["message"]
        == "Cannot access source files ['s3://doesnotexist', 's3://bucket/key', 'http://domain/file']"
    )


@pytest.mark.asyncio
async def test_put_latest(async_client):

    dataset = "test"
    response = await async_client.put(f"/dataset/{dataset}", json=payload)
    assert response.status_code == 201

    response = await async_client.put(
        f"/dataset/{dataset}/latest", json=version_payload
    )
    assert response.status_code == 400
    assert (
        response.json()["message"]
        == "You must list version name explicitly for this operation."
    )
