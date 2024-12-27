from typing import Any, Dict, List, Optional

import pytest
from fastapi import HTTPException
from httpx import AsyncClient

from app.models.pydantic.geostore import GeostoreCommon
from app.routes.thematic import geoencoder
from app.routes.thematic.geoencoder import _admin_boundary_lookup_sql, sanitize_names


@pytest.mark.asyncio
async def test_sanitize_names_pass_through() -> None:
    country = "A Country"
    region = "Some region"
    subregion = "SUBREGION"
    normalize = False

    names = sanitize_names(normalize, country, region, subregion)

    assert names == [country, region, subregion]


@pytest.mark.asyncio
async def test_sanitize_names_normalize() -> None:
    country = "Fictîcious de San México"
    region = "Söme Reğion"
    subregion = "SÜBREGION"
    normalize = True

    names = sanitize_names(normalize, country, region, subregion)

    assert names == ["ficticious de san mexico", "some region", "subregion"]


@pytest.mark.asyncio
async def test_sanitize_names_tolerate_empty() -> None:
    country = "México"
    region = "Tijuana"
    subregion = ""
    normalize = False

    names = sanitize_names(normalize, country, region, subregion)

    assert names == [country, region, None]


@pytest.mark.asyncio
async def test_sanitize_names_tolerate_enforce_hierarchy() -> None:
    country = "México"
    region = None
    subregion = "some subregion"
    normalize = False

    try:
        _ = sanitize_names(normalize, country, region, subregion)
    except HTTPException as e:
        assert (
            e.detail == "If subregion is specified, region must be specified as well."
        )


@pytest.mark.asyncio
async def test__admin_boundary_lookup_sql_country() -> None:
    sql = _admin_boundary_lookup_sql(
        "0", False, "some_dataset", "some_country", None, None
    )
    assert sql == (
        "SELECT gid_0, gid_1, gid_2, country, name_1, name_2 FROM some_dataset"
        " WHERE country='some_country' AND adm_level='0'"
    )


@pytest.mark.asyncio
async def test__admin_boundary_lookup_sql_country_region() -> None:
    sql = _admin_boundary_lookup_sql(
        "1", False, "some_dataset", "some_country", "some_region", None
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
        "2", False, "some_dataset", "some_country", "some_region", "some_subregion"
    )
    assert sql == (
        "SELECT gid_0, gid_1, gid_2, country, name_1, name_2 FROM some_dataset"
        " WHERE country='some_country'"
        " AND name_1='some_region'"
        " AND name_2='some_subregion'"
        " AND adm_level='2'"
    )


@pytest.mark.asyncio
async def test__admin_boundary_lookup_sql_all_normalized() -> None:
    sql = _admin_boundary_lookup_sql(
        "2", True, "some_dataset", "some_country", "some_region", "some_subregion"
    )
    assert sql == (
        "SELECT gid_0, gid_1, gid_2, country, name_1, name_2 FROM some_dataset"
        " WHERE country_normalized='some_country'"
        " AND name_1_normalized='some_region'"
        " AND name_2_normalized='some_subregion'"
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

    assert resp.json().get("message", {}).startswith("Invalid version name")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_geoencoder_nonexistant_version(async_client: AsyncClient) -> None:
    params = {"country": "Canada", "admin_version": "v4.0"}

    resp = await async_client.get("/thematic/geoencode", params=params)

    assert resp.json().get("message", {}).startswith("Version not found")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_geoencoder_nonexistant_version_lists_existing(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def mock_get_version_names(dataset: str):
        return [("fdkj",), ("dslj",), ("kfj",)]

    monkeypatch.setattr(geoencoder, "get_version_names", mock_get_version_names)

    params = {"country": "Canada", "admin_version": "v4.0"}
    resp = await async_client.get("/thematic/geoencode", params=params)

    assert resp.json().get("message", {}).endswith("['fdkj', 'dslj', 'kfj']")
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
        geoencoder, "_query_dataset_json", _query_dataset_json_mocked_no_results
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


@pytest.mark.asyncio
async def test_geoencoder_matches_full(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    admin_source = "gadm"
    admin_version = "v4.1"

    params = {
        "admin_source": admin_source,
        "admin_version": admin_version,
        "country": "Taiwan",
        "region": "Fujian",
        "subregion": "Kinmen",
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
            "matches": [
                {
                    "country": {"id": "TWN", "name": "Taiwan"},
                    "region": {"id": "TWN.1", "name": "Fujian"},
                    "subregion": {"id": "TWN.1.1", "name": "Kinmen"},
                }
            ],
        },
    }
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_geoencoder_matches_hide_extraneous(
    async_client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    admin_source = "gadm"
    admin_version = "v4.1"

    params = {
        "admin_source": admin_source,
        "admin_version": admin_version,
        "country": "Taiwan",
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
            "matches": [
                {
                    "country": {"id": "TWN", "name": "Taiwan"},
                    "region": {"id": None, "name": None},
                    "subregion": {"id": None, "name": None},
                }
            ],
        },
    }
    assert resp.status_code == 200


async def _query_dataset_json_mocked_no_results(
    dataset: str,
    version: str,
    sql: str,
    geostore: Optional[GeostoreCommon],
) -> List[Dict[str, Any]]:
    return []


async def _query_dataset_json_mocked_results(
    dataset: str,
    version: str,
    sql: str,
    geostore: Optional[GeostoreCommon],
) -> List[Dict[str, Any]]:
    return [
        {
            "gid_0": "TWN",
            "gid_1": "TWN.1_1",
            "gid_2": "TWN.1.1_1",
            "country": "Taiwan",
            "name_1": "Fujian",
            "name_2": "Kinmen",
        }
    ]
