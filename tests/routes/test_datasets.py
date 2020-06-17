import json

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


def test_datasets(meta_client, db):
    """
    Basic test to check if empty data api response as expected
    """

    dataset = "test"

    response = meta_client.get("/meta")
    assert response.status_code == 200
    assert response.json() == {"data": [], "status": "success"}

    response = meta_client.put(f"/meta/{dataset}", data=json.dumps(payload))
    assert response.status_code == 201
    assert response.json()["data"]["metadata"] == payload["metadata"]

    response = meta_client.get("/meta")
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()["data"][0]["metadata"] == payload["metadata"]

    response = meta_client.get(f"/meta/{dataset}")
    assert response.status_code == 200
    assert response.json()["data"]["metadata"] == payload["metadata"]

    cursor = db.execute(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :dataset;",
        {"dataset": dataset},
    )
    rows = cursor.fetchall()
    assert len(rows) == 1

    new_payload = {"metadata": {"title": "New Title"}}
    response = meta_client.patch(f"/meta/{dataset}", data=json.dumps(new_payload))
    assert response.status_code == 200
    assert response.json()["data"]["metadata"] != payload["metadata"]
    assert response.json()["data"]["metadata"]["title"] == "New Title"
    assert response.json()["data"]["metadata"]["subtitle"] == "string"

    response = meta_client.delete(f"/meta/{dataset}")
    assert response.status_code == 200
    assert response.json()["data"]["dataset"] == "test"

    cursor = db.execute(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :dataset;",
        {"dataset": dataset},
    )
    rows = cursor.fetchall()
    assert len(rows) == 0

    response = meta_client.get("/meta")
    assert response.status_code == 200
    assert response.json() == {"data": [], "status": "success"}
