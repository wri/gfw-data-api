from typing import Dict
from unittest.mock import Mock
from uuid import UUID

import pytest
from _pytest.monkeypatch import MonkeyPatch
from fastapi import HTTPException

from app.errors import InvalidResponseError, RecordNotFoundError
from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.geostore import Geometry, GeostoreCommon
from app.utils import geostore

rw_api_geostore_json: Dict = {
    "data": {
        "type": "geoStore",
        "id": "d8907d30eb5ec7e33a68aa31aaf918a4",
        "attributes": {
            "geojson": {
                "crs": {},
                "type": "FeatureCollection",
                "features": [
                    {
                        "geometry": {
                            "coordinates": [
                                [
                                    [13.286161423, 2.22263581],
                                    [13.895623684, 2.613460107],
                                    [14.475367069, 2.43969337],
                                    [15.288956165, 1.338479182],
                                    [13.44381094, 0.682623753],
                                    [13.286161423, 2.22263581],
                                ]
                            ],
                            "type": "Polygon",
                        },
                        "type": "Feature",
                    }
                ],
            },
            "hash": "d8907d30eb5ec7e33a68aa31aaf918a4",  # pragma: allowlist secret
            "provider": {},
            "areaHa": 2950164.393265342,
            "bbox": [13.286161423, 0.682623753, 15.288956165, 2.613460107],
            "lock": False,
            "info": {"use": {}},
        },
    }
}

data: Dict = rw_api_geostore_json["data"]["attributes"]
geojson: Dict = data["geojson"]["features"][0]["geometry"]
geometry: Geometry = Geometry.parse_obj(geojson)
geostore_common: GeostoreCommon = GeostoreCommon(
    geostore_id=data["hash"],
    geojson=geometry,
    area__ha=data["areaHa"],
    bbox=data["bbox"],
)


@pytest.mark.asyncio
async def test_get_geostore_from_any_origin_all_404s(monkeypatch: MonkeyPatch):
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a7"
    geostore_id_uuid = UUID(geostore_id_str)

    mock__get_gfw_geostore = Mock(geostore._get_gfw_geostore)
    mock__get_gfw_geostore.side_effect = RecordNotFoundError()
    monkeypatch.setattr(geostore, "_get_gfw_geostore", mock__get_gfw_geostore)

    mock_rw_get_geostore = Mock(geostore.rw_api.get_geostore)
    mock_rw_get_geostore.side_effect = RecordNotFoundError()
    monkeypatch.setattr(geostore.rw_api, "get_geostore", mock_rw_get_geostore)

    with pytest.raises(HTTPException) as h_e:
        _ = await geostore.get_geostore_from_any_origin(
            geostore_id_uuid, geostore_origin=GeostoreOrigin.rw
        )
    assert h_e.value.status_code == 404


@pytest.mark.asyncio
async def test_get_geostore_from_any_origin_gfw_success(monkeypatch: MonkeyPatch):
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a4"
    geostore_id_uuid = UUID(geostore_id_str)

    mock__get_gfw_geostore = Mock(geostore._get_gfw_geostore)
    mock__get_gfw_geostore.return_value = geostore_common
    monkeypatch.setattr(geostore, "_get_gfw_geostore", mock__get_gfw_geostore)

    mock_rw_get_geostore = Mock(geostore.rw_api.get_geostore)
    mock_rw_get_geostore.side_effect = RecordNotFoundError()
    monkeypatch.setattr(geostore.rw_api, "get_geostore", mock_rw_get_geostore)

    geo: GeostoreCommon = await geostore.get_geostore_from_any_origin(
        geostore_id_uuid, geostore_origin=GeostoreOrigin.rw
    )
    assert geo.geostore_id == geostore_id_uuid


@pytest.mark.asyncio
async def test_get_geostore_from_any_origin_rw_success(monkeypatch: MonkeyPatch):
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

    geo: GeostoreCommon = await geostore.get_geostore_from_any_origin(
        geostore_id_uuid, geostore_origin=GeostoreOrigin.rw
    )
    assert geo.geostore_id == geostore_id_uuid


@pytest.mark.asyncio
async def test_get_geostore_from_any_origin_mixed_errors(monkeypatch: MonkeyPatch):
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a7"
    geostore_id_uuid = UUID(geostore_id_str)

    mock__get_gfw_geostore = Mock(geostore._get_gfw_geostore)
    mock__get_gfw_geostore.side_effect = RecordNotFoundError()
    monkeypatch.setattr(geostore, "_get_gfw_geostore", mock__get_gfw_geostore)

    mock_rw_get_geostore = Mock(geostore.rw_api.get_geostore)
    mock_rw_get_geostore.side_effect = InvalidResponseError()
    monkeypatch.setattr(geostore.rw_api, "get_geostore", mock_rw_get_geostore)

    with pytest.raises(HTTPException) as h_e:
        _ = await geostore.get_geostore_from_any_origin(
            geostore_id_uuid, geostore_origin=GeostoreOrigin.rw
        )
    assert h_e.value.status_code == 500
