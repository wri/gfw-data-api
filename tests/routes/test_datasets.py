import json
from unittest.mock import patch

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


def test_datasets(client, db):
    """
    Basic test to check if empty data api response as expected
    """

    dataset = "test"

    response = client.get("/meta")
    assert response.status_code == 200
    assert response.json() == {"data": [], "status": "success"}

    response = client.put(f"/meta/{dataset}", data=json.dumps(payload))
    assert response.status_code == 201
    assert response.json()["data"]["metadata"] == payload["metadata"]

    response = client.get("/meta")
    assert response.status_code == 200
    assert len(response.json()["data"]) == 1
    assert response.json()["data"][0]["metadata"] == payload["metadata"]

    response = client.get(f"/meta/{dataset}")
    assert response.status_code == 200
    assert response.json()["data"]["metadata"] == payload["metadata"]

    cursor = db.execute(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :dataset;",
        {"dataset": dataset},
    )
    rows = cursor.fetchall()
    assert len(rows) == 1

    new_payload = {"metadata": {"title": "New Title"}}
    response = client.patch(f"/meta/{dataset}", data=json.dumps(new_payload))
    assert response.status_code == 200
    assert response.json()["data"]["metadata"] != payload["metadata"]
    assert response.json()["data"]["metadata"]["title"] == "New Title"
    assert response.json()["data"]["metadata"]["subtitle"] == "string"

    response = client.delete(f"/meta/{dataset}")
    assert response.status_code == 200
    assert response.json()["data"]["dataset"] == "test"

    cursor = db.execute(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :dataset;",
        {"dataset": dataset},
    )
    rows = cursor.fetchall()
    assert len(rows) == 0

    response = client.get("/meta")
    assert response.status_code == 200
    assert response.json() == {"data": [], "status": "success"}


@patch("fastapi.BackgroundTasks.add_task", return_value=None)
def test_dataset_delete_protection(mocked_task, client):
    dataset = "test"
    version = "v1.1.1"

    client.put(f"/meta/{dataset}", data=json.dumps(payload))

    version_payload = {
        "is_latest": True,
        "source_type": "vector",
        "source_uri": ["s3://some/path"],
        "metadata": payload["metadata"],
        "creation_options": {"src_driver": "ESRI Shapefile", "zipped": True},
    }

    # with patch("app.tasks.default_assets.create_default_asset", return_value=True) as mock_asset:
    client.put(f"/meta/{dataset}/{version}", data=json.dumps(version_payload))

    response = client.delete(f"/meta/{dataset}")

    assert response.status_code == 409

    client.delete(f"/meta/{dataset}/{version}")
    response = client.delete(f"/meta/{dataset}")

    assert response.status_code == 200
    assert mocked_task.called
