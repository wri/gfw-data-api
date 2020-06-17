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
        "added_date": "string",
        "why_added": "string",
        "other": "string",
        "learn_more": "string",
    }
}


# @patch("app.tasks.default_assets.create_default_asset", return_value=True)
@patch("fastapi.BackgroundTasks.add_task", return_value=None)
def test_versions(mocked_task, client, db):
    """
    Test version path operations.
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
