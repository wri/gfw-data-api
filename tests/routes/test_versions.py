import json
from unittest.mock import patch

from app.models.pydantic.metadata import VersionMetadata

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
def test_versions(mocked_task, client, db):
    """Test version path operations.

    We patch/ disable background tasks here, as they run asynchronously.
    Such tasks are tested separately in a different module
    """
    dataset = "test"
    version = "v1.1.1"

    response = client.put(f"/meta/{dataset}", data=json.dumps(payload))
    assert response.status_code == 201
    assert response.json()["data"]["metadata"] == payload["metadata"]
    assert response.json()["data"]["versions"] == []

    version_payload = {
        "is_latest": True,
        "source_type": "vector",
        "source_uri": ["s3://some/path"],
        "metadata": payload["metadata"],
        "creation_options": {"src_driver": "ESRI Shapefile", "zipped": True},
    }

    # with patch("app.tasks.default_assets.create_default_asset", return_value=True) as mock_asset:
    response = client.put(
        f"/meta/{dataset}/{version}", data=json.dumps(version_payload)
    )
    version_data = response.json()
    assert response.status_code == 202
    assert version_data["data"]["dataset"] == dataset
    assert version_data["data"]["version"] == version
    assert version_data["data"]["is_latest"] is True
    assert version_data["data"]["metadata"] == VersionMetadata(**payload["metadata"])
    assert mocked_task.called

    # Check if the latest endpoint redirects us to v1.1.1
    response = client.get(f"/meta/{dataset}/latest?test=test&test1=test1")
    assert response.json()["data"]["version"] == "v1.1.1"


@patch("fastapi.BackgroundTasks.add_task", return_value=None)
def test_version_metadata(mocked_task, client):
    """Test if Version inherits metadata from Dataset.

    Version should be able to overwrite any metadata attribute
    """
    dataset = "test"
    version = "v1.1.1"

    dataset_metadata = {"title": "Title", "subtitle": "Subtitle"}

    response = client.put(
        f"/meta/{dataset}", data=json.dumps({"metadata": dataset_metadata})
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
        "is_latest": True,
        "source_type": "vector",
        "source_uri": ["s3://some/path"],
        "metadata": version_metadata,
        "creation_options": {"src_driver": "ESRI Shapefile", "zipped": True},
    }

    response = client.put(
        f"/meta/{dataset}/{version}", data=json.dumps(version_payload)
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

    response = client.get(f"/meta/{dataset}/{version}")
    assert response.json()["data"]["metadata"] == result_metadata

    response = client.delete(f"/meta/{dataset}/{version}")
    assert response.json()["data"]["metadata"] == result_metadata


@patch("fastapi.BackgroundTasks.add_task", return_value=None)
def test_version_delete_protection(mocked_task, client):
    dataset = "test"
    version1 = "v1.1.1"
    version2 = "v1.1.2"

    client.put(f"/meta/{dataset}", data=json.dumps(payload))

    version_payload = {
        "is_latest": True,
        "source_type": "vector",
        "source_uri": ["s3://some/path"],
        "metadata": payload["metadata"],
        "creation_options": {"src_driver": "ESRI Shapefile", "zipped": True},
    }

    # with patch("app.tasks.default_assets.create_default_asset", return_value=True) as mock_asset:
    client.put(f"/meta/{dataset}/{version1}", data=json.dumps(version_payload))

    client.put(f"/meta/{dataset}/{version2}", data=json.dumps(version_payload))

    client.patch(f"/meta/{dataset}/{version2}", data=json.dumps({"is_latest": True}))

    response = client.delete(f"/meta/{dataset}/{version2}")

    assert response.status_code == 409

    client.delete(f"/meta/{dataset}/{version1}")
    response = client.delete(f"/meta/{dataset}/{version2}")
    assert response.status_code == 200
    assert mocked_task.called


@patch("fastapi.BackgroundTasks.add_task", return_value=None)
def test_latest_middleware(mocked_task, client):
    """Test if middleware redirects to correct version when using `latest`
    version identifier."""

    dataset = "test"
    version = "v1.1.1"

    response = client.put(f"/meta/{dataset}", data=json.dumps(payload))
    print(response.json())
    assert response.status_code == 201

    version_payload = {
        "is_latest": False,
        "source_type": "vector",
        "source_uri": ["s3://some/path"],
        "metadata": payload["metadata"],
        "creation_options": {"src_driver": "ESRI Shapefile", "zipped": True},
    }

    response = client.put(
        f"/meta/{dataset}/{version}", data=json.dumps(version_payload)
    )
    print(response.json())
    assert response.status_code == 202

    response = client.get(f"/meta/{dataset}/{version}")
    print(response.json())
    assert response.status_code == 200

    response = client.get(f"/meta/{dataset}/latest")
    print(response.json())
    assert response.status_code == 404

    response = client.patch(
        f"/meta/{dataset}/{version}", data=json.dumps({"is_latest": True})
    )
    print(response.json())
    assert response.status_code == 200
    assert response.json()["data"]["is_latest"] is True

    response = client.get(f"/meta/{dataset}/latest")
    print(response.json())
    assert response.status_code == 200
    assert response.json()["data"]["version"] == version
