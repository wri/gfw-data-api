from typing import Any, Dict, List, Optional

import pytest
from httpx import AsyncClient

from app.models.pydantic.geostore import GeostoreCommon
from app.routes.thematic import geoencoder
from app.routes.thematic.geoencoder import _admin_boundary_lookup_sql


@pytest.mark.asyncio
async def test__admin_boundary_lookup_sql_country() -> None:
    sql = _admin_boundary_lookup_sql(False, "some_dataset", "some_country", None, None)
    assert sql == (
        "SELECT gid_0, gid_1, gid_2, country, name_1, name_2 FROM some_dataset"
        " WHERE country='some_country' AND adm_level='0'"
    )


@pytest.mark.asyncio
async def test__admin_boundary_lookup_sql_country_region() -> None:
    sql = _admin_boundary_lookup_sql(
        False, "some_dataset", "some_country", "some_region", None
    )
    assert sql == (
        "SELECT gid_0, gid_1, gid_2, country, name_1, name_2 FROM some_dataset"
        " WHERE country='some_country'"
        " AND name_1='some_region'"
        " AND adm_level='1'"
    )


@pytest.mark.asyncio
async def test__admin_boundary_lookup_sql_all() -> None:
    sql = _admin_boundary_lookup_sql(
        False, "some_dataset", "some_country", "some_region", "some_subregion"
    )
    assert sql == (
        "SELECT gid_0, gid_1, gid_2, country, name_1, name_2 FROM some_dataset"
        " WHERE country='some_country'"
        " AND name_1='some_region'"
        " AND name_2='some_subregion'"
        " AND adm_level='2'"
    )


@pytest.mark.asyncio
async def test_geoencoder_no_admin_version(async_client: AsyncClient) -> None:
    params = {"country": "Canada"}

    resp = await async_client.get("/thematic/geoencode", params=params)

    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_geoencoder_invalid_version_pattern(async_client: AsyncClient) -> None:
    params = {"country": "Canada", "admin_version": "fails_regex"}

    resp = await async_client.get("/thematic/geoencode", params=params)

    assert resp.json().get("message", {}).startswith("Invalid version")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_geoencoder_nonexistant_version(async_client: AsyncClient) -> None:
    params = {"country": "Canada", "admin_version": "v4.0"}

    resp = await async_client.get("/thematic/geoencode", params=params)

    assert resp.json().get("message", {}).startswith("Version not found")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_geoencoder_bad_boundary_source(async_client: AsyncClient) -> None:
    params = {
        "admin_source": "bobs_boundaries",
        "admin_version": "4.1",
        "country": "Canadiastan",
    }

    resp = await async_client.get("/thematic/geoencode", params=params)

    assert resp.json().get("message", {}).startswith("Invalid admin boundary source")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_geoencoder_no_matches(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    admin_source = "gadm"
    admin_version = "v4.1"

    params = {
        "admin_source": admin_source,
        "admin_version": admin_version,
        "country": "Canadiastan",
    }

    async def mock_version_is_valid(dataset: str, version: str):
        return None

    monkeypatch.setattr(geoencoder, "version_is_valid", mock_version_is_valid)
    monkeypatch.setattr(
        geoencoder, "_query_dataset_json", _query_dataset_json_mocked_results
    )

    resp = await async_client.get("/thematic/geoencode", params=params)

    assert resp.json() == {
        "status": "success",
        "data": {
            "adminSource": admin_source,
            "adminVersion": admin_version,
            "matches": [],
        },
    }
    assert resp.status_code == 200


async def _query_dataset_json_mocked_results(
    dataset: str,
    version: str,
    sql: str,
    geostore: Optional[GeostoreCommon],
) -> List[Dict[str, Any]]:
    return []
