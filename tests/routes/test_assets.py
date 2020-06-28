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
        "asset_type": "Vector tile cache",
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
    # assert on the message too

    # Now add a task changelog of status "failed" which should make the
    # version status "failed". Try to add a non-default asset again, which
    # should fail as well but with a different explanation.
    create_asset_resp = client.post(
        f"/meta/{dataset}/{version}/assets", json=asset_payload
    )
    assert create_asset_resp.json()["status"] == "fail"
    # assert on the message too
