import uuid
from typing import Any, Dict, Tuple
from uuid import UUID

import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient

from app.crud import api_keys
from tests_v2.unit.app.routes.utils import assert_is_datetime, assert_jsend
from tests_v2.utils import void_coroutine

TEST_DATA = [
    (
        "My first API Key",
        "Global Forest Watch",
        "support@globalforestwatch.org",
        ["www.globalforestwatch.org"],
        False,
    ),
    (
        "My first API Key",
        "Global Forest Watch",
        "support@globalforestwatch.org",
        [],
        False,
    ),
    (
        "My first API Key",
        "Global Forest Watch",
        "support@globalforestwatch.org",
        ["www.globalforestwatch.org"],
        True,
    ),
    (
        "My first API Key",
        "Global Forest Watch",
        "support@globalforestwatch.org",
        [],
        True,
    ),
]


@pytest.mark.parametrize(
    "alias, organization, email, domains, never_expires", TEST_DATA
)
@pytest.mark.asyncio
async def test_create_apikey(
    alias,
    organization,
    email,
    domains,
    never_expires,
    async_client: AsyncClient,
    monkeypatch: MonkeyPatch,
):
    payload = {
        "alias": alias,
        "email": email,
        "organization": organization,
        "domains": domains,
        "never_expires": never_expires,
    }
    monkeypatch.setattr(api_keys, "add_api_key_to_gateway", void_coroutine)
    response = await async_client.post("/auth/apikey", json=payload)

    assert_jsend(response.json())
    print(response.text)
    assert response.status_code == 201

    _validate_response(
        response.json()["data"], alias, email, organization, domains, never_expires
    )


@pytest.mark.parametrize(
    "alias, organization, email, domains, never_expires", TEST_DATA
)
@pytest.mark.asyncio
async def test_create_apikey_no_admin(
    alias,
    organization,
    email,
    domains,
    never_expires,
    async_client_no_admin: AsyncClient,
    monkeypatch: MonkeyPatch,
):
    payload = {
        "alias": alias,
        "email": email,
        "organization": organization,
        "domains": domains,
        "never_expires": never_expires,
    }
    monkeypatch.setattr(api_keys, "add_api_key_to_gateway", void_coroutine)

    response = await async_client_no_admin.post("/auth/apikey", json=payload)

    assert_jsend(response.json())

    if never_expires:
        assert response.status_code == 400
    else:
        print(response.text)
        assert response.status_code == 201
        _validate_response(
            response.json()["data"], alias, email, organization, domains, never_expires
        )


@pytest.mark.parametrize(
    "alias, organization, email, domains, never_expires", [TEST_DATA[0]]
)
@pytest.mark.asyncio
async def test_create_apikey_unauthenticated(
    alias,
    organization,
    email,
    domains,
    never_expires,
    async_client_unauthenticated: AsyncClient,
    monkeypatch: MonkeyPatch,
):
    payload = {
        "alias": alias,
        "email": email,
        "organization": organization,
        "domains": domains,
        "never_expires": never_expires,
    }
    monkeypatch.setattr(api_keys, "add_api_key_to_gateway", void_coroutine)

    response = await async_client_unauthenticated.post("/auth/apikey", json=payload)

    assert_jsend(response.json())

    print(response.text)
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_apikey(
    apikey: Tuple[UUID, Dict[str, Any]], async_client: AsyncClient
):
    api_key, payload = apikey
    response = await async_client.get(f"/auth/apikey/{api_key}")
    assert_jsend(response.json())

    assert response.status_code == 200
    assert response.json()["data"]["api_key"] == api_key
    _validate_response(response.json()["data"], **payload)


@pytest.mark.asyncio
async def test_get_apikey_not_exists(
    apikey: Tuple[UUID, Dict[str, Any]], async_client: AsyncClient
):
    api_key = uuid.uuid4()
    response = await async_client.get(f"/auth/apikey/{api_key}")
    assert_jsend(response.json())

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_apikey_no_admin(
    apikey: Tuple[UUID, Dict[str, Any]], async_client_no_admin: AsyncClient
):
    api_key, payload = apikey
    response = await async_client_no_admin.get(f"/auth/apikey/{api_key}")
    assert_jsend(response.json())
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_apikeys(
    apikey: Tuple[UUID, Dict[str, Any]], async_client: AsyncClient
):
    api_key, payload = apikey
    response = await async_client.get("/auth/apikeys")
    assert_jsend(response.json())

    assert response.status_code == 200
    assert response.json()["data"][0]["api_key"] == api_key
    _validate_response(response.json()["data"][0], **payload)


@pytest.mark.parametrize("source", ["origin", "referrer"])
@pytest.mark.asyncio
async def test_validate_apikey(
    source, apikey: Tuple[UUID, Dict[str, Any]], async_client: AsyncClient
):
    api_key, payload = apikey
    origin = "https://" + payload["domains"][0]
    params = {source: origin}
    response = await async_client.get(f"/auth/apikey/{api_key}/validate", params=params)
    assert_jsend(response.json())
    assert response.json()["data"]["is_valid"]


@pytest.mark.parametrize("source", ["origin", "referrer"])
@pytest.mark.asyncio
async def test_validate_apikey_not_valid(
    source: str, apikey: Tuple[UUID, Dict[str, Any]], async_client: AsyncClient
):
    api_key, payload = apikey
    origin = "https://www.test.com"
    params = {source: origin}
    response = await async_client.get(f"/auth/apikey/{api_key}/validate", params=params)
    assert_jsend(response.json())
    assert not response.json()["data"]["is_valid"]


@pytest.mark.asyncio
async def test_get_apikeys_no_keys(async_client: AsyncClient):
    response = await async_client.get("/auth/apikeys")
    assert_jsend(response.json())

    assert response.status_code == 200
    assert response.json()["data"] == list()


@pytest.mark.asyncio
async def test_delete_apikey(
    apikey: Tuple[UUID, Dict[str, Any]],
    async_client: AsyncClient,
    monkeypatch: MonkeyPatch,
):
    api_key, payload = apikey
    monkeypatch.setattr(api_keys, "delete_api_key_from_gateway", void_coroutine)
    response = await async_client.delete(f"/auth/apikey/{api_key}")
    assert_jsend(response.json())

    assert response.status_code == 200
    assert response.json()["data"]["api_key"] == api_key
    _validate_response(response.json()["data"], **payload)


@pytest.mark.asyncio
async def test_delete_apikey_other_user(
    apikey: Tuple[UUID, Dict[str, Any]], async_client_no_admin: AsyncClient
):
    api_key, payload = apikey
    response = await async_client_no_admin.delete(f"/auth/apikey/{api_key}")
    assert_jsend(response.json())

    assert response.status_code == 403


def _validate_response(data, alias, email, organization, domains, never_expires):
    UUID(data["api_key"])  # check if this is an UUID
    assert data["alias"] == alias
    assert data["email"] == email
    assert data["organization"] == organization
    assert data["domains"] == domains
    assert_is_datetime(data["created_on"])
    assert_is_datetime(data["updated_on"])

    if never_expires:
        assert data["expires_on"] is None
    else:
        assert_is_datetime(data["expires_on"])
