from tests.routes import create_default_asset


def test_assets(client, db):
    """Basic tests of asset endpoint behavior."""
    # Add a dataset, version, and default asset
    dataset = "test"
    version = "v20200626"

    asset = create_default_asset(client, dataset, version)
    asset_id = asset["asset_id"]

    # Verify that the asset and version are in state "pending"
    version_resp = client.get(f"/meta/{dataset}/{version}")
    assert version_resp.json()["data"]["status"] == "pending"

    asset_resp = client.get(f"/meta/{dataset}/{version}/assets/{asset_id}")
    assert asset_resp.json()["data"]["status"] == "pending"

    # Try adding a non-default asset, which shouldn't work while the version
    # is still in "pending" status
    asset_payload = {
        "asset_type": "Database table",
        "asset_uri": "http://www.slashdot.org",
        "is_managed": False,
        "creation_options": {
            "zipped": False,
            "src_driver": "GeoJSON",
            "delimiter": ",",
        },
    }
    create_asset_resp = client.post(
        f"/meta/{dataset}/{version}/assets", json=asset_payload
    )
    assert create_asset_resp.json()["status"] == "fail"
    assert create_asset_resp.json()["data"] == (
        "Version status is currently `pending`. "
        "Please retry once version is in status `saved`"
    )

    # Now add a task changelog of status "failed" which should make the
    # version status "failed". Try to add a non-default asset again, which
    # should fail as well but with a different explanation.
    tasks = client.get(f"/tasks/assets/{asset_id}").json()["data"]
    sample_task_id = tasks[0]["task_id"]
    patch_payload = {
        "change_log": [
            {
                "date_time": "2020-06-25 14:30:00",
                "status": "failed",
                "message": "Bad Luck!",
                "detail": "None",
            }
        ]
    }
    patch_resp = client.patch(f"/tasks/{sample_task_id}", json=patch_payload)
    assert patch_resp.json()["status"] == "success"

    create_asset_resp = client.post(
        f"/meta/{dataset}/{version}/assets", json=asset_payload
    )
    assert create_asset_resp.json()["status"] == "fail"
    assert create_asset_resp.json()["data"] == (
        "Version status is `failed`. Cannot add any assets."
    )
