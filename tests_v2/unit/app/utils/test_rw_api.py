from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import Response

from app.errors import InvalidResponseError, RecordNotFoundError
from app.models.pydantic.geostore import GeostoreCommon
from app.utils.rw_api import get_geostore
from tests_v2.fixtures.sample_rw_geostore_response import response_body


@pytest.mark.asyncio
async def test_get_geostore_success(monkeypatch: MonkeyPatch):
    # Just to make sure we're not actually hitting the RW API,
    # ask for a non-existent geostore
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a5"
    geostore_id_uuid = UUID(geostore_id_str)

    mock_get = AsyncMock()
    mock_get.return_value = Response(200, json=response_body)
    monkeypatch.setattr("httpx.AsyncClient.get", mock_get)

    geo: GeostoreCommon = await get_geostore(geostore_id_uuid)

    # Compare against the id actually in the response fixture, above,
    # which has the payload of a valid geostore
    assert geo.geostore_id == UUID("d8907d30eb5ec7e33a68aa31aaf918a4")


@pytest.mark.asyncio
async def test_get_geostore_404(monkeypatch: MonkeyPatch):
    # Just to make sure we're not actually hitting the RW API,
    # ask for a valid geostore
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a4"
    geostore_id_uuid = UUID(geostore_id_str)

    mock_get = AsyncMock()
    mock_get.return_value = Response(
        404, json={"errors": [{"status": 404, "detail": "GeoStore not found"}]}
    )
    monkeypatch.setattr("httpx.AsyncClient.get", mock_get)

    with pytest.raises(RecordNotFoundError):
        _ = await get_geostore(geostore_id_uuid)


@pytest.mark.asyncio
async def test_get_geostore_other(monkeypatch: MonkeyPatch):
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a6"
    geostore_id_uuid = UUID(geostore_id_str)

    mock_get = AsyncMock()
    mock_get.return_value = Response(
        666, json={"errors": [{"status": 666, "detail": "Bad stuff"}]}
    )
    monkeypatch.setattr("httpx.AsyncClient.get", mock_get)

    with pytest.raises(InvalidResponseError):
        _ = await get_geostore(geostore_id_uuid)
