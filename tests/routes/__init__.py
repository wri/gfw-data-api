import json
from typing import Any, Dict
from uuid import UUID

from fastapi.encoders import jsonable_encoder

basic_metadata = {
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

# version_creation_data = {
#     "is_latest": True,
#     "source_type": "vector",
#     "source_uri": ["s3://some/path"],
#     "metadata": basic_metadata['metadata'],
#     "creation_options": {"src_driver": "ESRI Shapefile", "zipped": True},
# }


def create_version(test_client, dataset, version) -> None:
    # Create dataset and version
    dataset_resp = test_client.put(f"/meta/{dataset}", data=json.dumps(basic_metadata))
    assert dataset_resp.json()["status"] == "success"

    version_payload = {
        "is_latest": True,
        "source_type": "vector",
        "source_uri": ["s3://some/path"],
        "metadata": {},
        "creation_options": {"src_driver": "ESRI Shapefile", "zipped": True},
    }
    version_response = test_client.put(
        f"/meta/{dataset}/{version}", data=json.dumps(version_payload)
    )
    assert version_response.json()["status"] == "success"


def create_asset(test_client, dataset, version, asset_type, asset_uri):
    # Create dataset and version records
    create_version(test_client, dataset, version)

    resp = test_client.get(f"/meta/{dataset}/{version}/assets")
    assert len(resp.json()["data"]) == 1
    assert resp.json()["status"] == "success"

    return resp.json()["data"][0]

    # asset_payload = {
    #     "asset_type": asset_type,
    #     "asset_uri": asset_uri,
    #     "is_managed": False,
    #     "creation_options": {
    #         "zipped": False,
    #         "src_driver": "GeoJSON",
    #         "delimiter": ","
    #     }
    # }
    #
    # resp = test_client.post(
    #     f"/meta/{dataset}/{version}/assets", data=json.dumps(asset_payload)
    # )
    # return resp
