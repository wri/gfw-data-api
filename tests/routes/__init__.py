from typing import Any, Dict

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


def create_version(test_client, dataset, version) -> None:
    # Create dataset and version
    dataset_resp = test_client.put(f"/meta/{dataset}", json=generic_dataset_metadata)
    assert dataset_resp.json()["status"] == "success"

    version_payload = {
        "is_latest": True,
        "source_type": "vector",
        "source_uri": ["s3://some/path"],
        "metadata": {},
        "creation_options": {"src_driver": "ESRI Shapefile", "zipped": True},
    }
    version_response = test_client.put(
        f"/meta/{dataset}/{version}", json=version_payload
    )
    assert version_response.json()["status"] == "success"


def create_default_asset(test_client, dataset, version) -> Dict[str, Any]:
    # Create dataset and version records. A default asset is created
    # automatically when the version is created.
    create_version(test_client, dataset, version)

    resp = test_client.get(f"/meta/{dataset}/{version}/assets")
    assert len(resp.json()["data"]) == 1
    assert resp.json()["status"] == "success"

    return resp.json()["data"][0]
