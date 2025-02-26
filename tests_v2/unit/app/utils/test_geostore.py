from unittest.mock import Mock
from uuid import UUID

import pytest
from _pytest.monkeypatch import MonkeyPatch
from fastapi import HTTPException

from app.errors import InvalidResponseError, RecordNotFoundError
from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.geostore import GeostoreCommon
from app.utils import geostore
from app.utils.gadm import extract_level_gid
from tests_v2.fixtures.sample_rw_geostore_response import geostore_common


@pytest.mark.asyncio
async def test_get_geostore_all_404s(monkeypatch: MonkeyPatch):
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a7"
    geostore_id_uuid = UUID(geostore_id_str)

    mock__get_gfw_geostore = Mock(geostore._get_gfw_geostore)
    mock__get_gfw_geostore.side_effect = RecordNotFoundError()
    monkeypatch.setattr(geostore, "_get_gfw_geostore", mock__get_gfw_geostore)

    mock_rw_get_geostore = Mock(geostore.rw_api.get_geostore)
    mock_rw_get_geostore.side_effect = RecordNotFoundError()
    monkeypatch.setattr(geostore.rw_api, "get_geostore", mock_rw_get_geostore)

    with pytest.raises(HTTPException) as e:
        _ = await geostore.get_geostore(
            geostore_id_uuid, geostore_origin=GeostoreOrigin.rw
        )
    assert e.value.status_code == 404


@pytest.mark.asyncio
async def test_get_geostore_gfw_success(monkeypatch: MonkeyPatch):
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a4"
    geostore_id_uuid = UUID(geostore_id_str)

    mock__get_gfw_geostore = Mock(geostore._get_gfw_geostore)
    mock__get_gfw_geostore.return_value = geostore_common
    monkeypatch.setattr(geostore, "_get_gfw_geostore", mock__get_gfw_geostore)

    mock_rw_get_geostore = Mock(geostore.rw_api.get_geostore)
    mock_rw_get_geostore.side_effect = RecordNotFoundError()
    monkeypatch.setattr(geostore.rw_api, "get_geostore", mock_rw_get_geostore)

    geo: GeostoreCommon = await geostore.get_geostore(
        geostore_id_uuid, geostore_origin=GeostoreOrigin.rw
    )
    assert geo.geostore_id == geostore_id_uuid


@pytest.mark.asyncio
async def test_get_geostore_rw_success(monkeypatch: MonkeyPatch):
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a4"
    geostore_id_uuid = UUID(geostore_id_str)

    mock__get_gfw_geostore = Mock(
        geostore._get_gfw_geostore, side_effect=RecordNotFoundError
    )
    monkeypatch.setattr(geostore, "_get_gfw_geostore", mock__get_gfw_geostore)

    mock_rw_get_geostore = Mock(
        geostore.rw_api.get_geostore, return_value=geostore_common
    )
    monkeypatch.setattr(geostore.rw_api, "get_geostore", mock_rw_get_geostore)

    geo: GeostoreCommon = await geostore.get_geostore(
        geostore_id_uuid, geostore_origin=GeostoreOrigin.gfw
    )
    assert geo.geostore_id == geostore_id_uuid


@pytest.mark.asyncio
async def test_get_geostore_mixed_errors(monkeypatch: MonkeyPatch):
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a7"
    geostore_id_uuid = UUID(geostore_id_str)

    mock__get_gfw_geostore = Mock(geostore._get_gfw_geostore)
    mock__get_gfw_geostore.side_effect = RecordNotFoundError()
    monkeypatch.setattr(geostore, "_get_gfw_geostore", mock__get_gfw_geostore)

    mock_rw_get_geostore = Mock(geostore.rw_api.get_geostore)
    mock_rw_get_geostore.side_effect = InvalidResponseError()
    monkeypatch.setattr(geostore.rw_api, "get_geostore", mock_rw_get_geostore)

    with pytest.raises(HTTPException) as e:
        _ = await geostore.get_geostore(
            geostore_id_uuid, geostore_origin=GeostoreOrigin.rw
        )
    assert e.value.status_code == 500


@pytest.mark.asyncio
async def test_extract_level_gid() -> None:
    # Normal gid values
    match1 = "USA.5.10_1"
    assert extract_level_gid(0, match1) == "USA"
    assert extract_level_gid(1, match1) == "5"
    assert extract_level_gid(2, match1) == "10"

    # Ghana values with bad formatting (missing dot after GHA in gadm 4.1)
    match2 = "GHA7.1_2"
    assert extract_level_gid(0, match2) == "GHA"
    assert extract_level_gid(1, match2) == "7"
    assert extract_level_gid(2, match2) == "1"

    # Indonesia values with bad formatting (missing suffix _1 in gadm 4.1)
    match3 = "IDN.35.4"
    assert extract_level_gid(0, match3) == "IDN"
    assert extract_level_gid(1, match3) == "35"
    assert extract_level_gid(2, match3) == "4"
