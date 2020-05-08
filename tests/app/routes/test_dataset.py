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


def test_datasets(client):
    """
    Basic test to check if empty data api response as expected
    """
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == []

    response = client.put("/test", data=json.dumps(payload))
    print(response.json())
    assert response.status_code == 201
    assert response.json()["metadata"] == payload["metadata"]

    response = client.get("/")
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["metadata"] == payload["metadata"]

    response = client.get("/test")
    assert response.status_code == 200
    assert response.json()["metadata"] == payload["metadata"]
