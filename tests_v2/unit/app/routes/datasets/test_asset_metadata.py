import pytest
from httpx import AsyncClient

from app.crud import assets as assets_crud
from app.authentication import token
from app.authentication.token import get_manager, get_user
from tests_v2.utils import (
    get_admin_mocked,
    get_manager_mocked,
    get_user_mocked,
    raises_401,
)
from app.main import app as appmain


@pytest.mark.asyncio
async def test_get_asset_metadata(
    generic_vector_source_version,
    async_client: AsyncClient,
) -> None:
    # the vector source fixture creates a vector asset record and its associated
    # dynamic tile cache asset with the `fields` metadata properties populated
    # so testing against those records
    dataset, version, _ = generic_vector_source_version
    resp = await async_client.get(f"dataset/{dataset}/{version}/assets")

    metadata = resp.json()["data"][0]["metadata"]
    _ = metadata.pop("id")

    assert len(metadata["fields"]) == 2
    fid, geom = metadata["fields"]
    assert fid["name"] == "fid"
    assert fid["data_type"] == "integer"
    assert geom["name"] == "geom"
    assert geom["data_type"] == "geometry"


@pytest.mark.asyncio
async def test_update_asset_metadata(
    generic_vector_source_version,
):
    max_zoom_before_update = 14
    max_zoom = 12
    dataset, version, _ = generic_vector_source_version
    assets = await assets_crud.get_assets_by_filter(
        dataset, version, asset_types=["Dynamic vector tile cache"]
    )
    assert (len(assets)) == 1

    (tile_cache_asset,) = assets
    assert tile_cache_asset.metadata.max_zoom == max_zoom_before_update

    # generic_vector_source_version creates with owner of manager_mocked, so
    # update by manager_mocked should succeed
    appmain.dependency_overrides[get_user] = get_manager_mocked

    async with AsyncClient(
        app=appmain,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as async_client:
        resp = await async_client.patch(
            f"asset/{tile_cache_asset.asset_id}/metadata", json={"max_zoom": max_zoom}
        )
    
        assert resp.json()["status"] == "success"
        assert resp.json()["data"]["max_zoom"] == max_zoom

    # update by user_mocked should fail
    appmain.dependency_overrides = {}
    appmain.dependency_overrides[get_user] = get_user_mocked

    async with AsyncClient(
        app=appmain,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as async_client:
        resp = await async_client.patch(
            f"asset/{tile_cache_asset.asset_id}/metadata", json={"max_zoom": max_zoom}
        )
    
        assert resp.json()["status"] == "failed"
        assert resp.status_code == 401

    appmain.dependency_overrides = {}


@pytest.mark.asyncio
async def test_invalid_field_returns_422_code(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version
    assets = await assets_crud.get_assets_by_filter(
        dataset, version, asset_types=["Dynamic vector tile cache"]
    )
    assert (len(assets)) == 1

    (tile_cache_asset,) = assets

    resp = await async_client.patch(
        f"asset/{assets[0].asset_id}/metadata", json={"fake_field": 2}
    )
    assert resp.status_code == 422

    # mixing fields from different asset types should also fail
    resp = await async_client.patch(
        f"asset/{tile_cache_asset.asset_id}/metadata",
        json={"min_zoom": 2, "resolution": 3},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_field_metadata(
    generic_vector_source_version, async_client: AsyncClient
):
    dataset, version, _ = generic_vector_source_version
    assets = await assets_crud.get_assets_by_filter(
        dataset, version, asset_types=["Dynamic vector tile cache"]
    )
    assert (len(assets)) == 1

    tile_cache_asset = assets[0]
    resp = await async_client.get(f"asset/{tile_cache_asset.asset_id}/fields/geom")

    assert resp.json()["data"]["name"] == "geom"
    assert resp.json()["data"]["data_type"] == "geometry"


@pytest.mark.asyncio
async def test_update_field_metadata(
    generic_vector_source_version
):
    field_description = "geometry field"
    dataset, version, _ = generic_vector_source_version
    assets = await assets_crud.get_assets_by_filter(
        dataset, version, asset_types=["Dynamic vector tile cache"]
    )
    assert (len(assets)) == 1

    tile_cache_asset = assets[0]
    assert tile_cache_asset.metadata.fields[1].description is None
    assert tile_cache_asset.metadata.fields[1].name == "geom"

    appmain.dependency_overrides[get_user] = get_manager_mocked

    async with AsyncClient(
        app=appmain,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as async_client:
        resp = await async_client.patch(
            f"asset/{tile_cache_asset.asset_id}/fields/geom",
            json={"description": field_description},
        )

        assert resp.json()["status"] == "success"
        assert resp.json()["data"]["description"] == field_description

    appmain.dependency_overrides = {}
    appmain.dependency_overrides[get_user] = get_user_mocked

    async with AsyncClient(
        app=appmain,
        base_url="http://test",
        trust_env=False,
        headers={"Origin": "https://www.globalforestwatch.org"},
    ) as async_client:
        resp = await async_client.patch(
            f"asset/{tile_cache_asset.asset_id}/fields/geom",
            json={"description": field_description},
        )

        assert resp.json()["status"] == "failed"
        assert resp.status_code == 401

    appmain.dependency_overrides = {}
