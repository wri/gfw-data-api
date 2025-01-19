from functools import partial

import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient, MockTransport, Request, Response

from app.models.pydantic.geostore import RWGeostoreResponse
from app.routes.geostore import geostore
from app.utils import rw_api

example_admin_list = {
    "data": [
        {
            "geostoreId": "2c7cbecf8f8e0016a6b8faf74c788a19",  # pragma: allowlist secret
            "iso": "ABW",
            "name": "Aruba",
        },
        {
            "geostoreId": "23b11bc0d0b417d3af08bca543fc2d0b",  # pragma: allowlist secret
            "iso": "ABW",
        },  # pragma: allowlist secret
    ]
}


example_geostore_resp = {
    "data": {
        "type": "geoStore",
        "id": "88db597b6bcd096fb80d1542cdc442be",  # pragma: allowlist secret
        "attributes": {
            "geojson": {
                "crs": {},
                "type": "FeatureCollection",
                "features": [
                    {
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [-6.01707501, 36.207626197],
                                        [-6.013556338, 36.207692397],
                                        [-6.008845712, 36.206873788],
                                        [-6.008381604, 36.207045425],
                                        [-6.00701305, 36.20720151],
                                    ]
                                ]
                            ],
                        },
                        "type": "Feature",
                        "properties": None,
                    }
                ],
            },
            "hash": "88db597b6bcd096fb80d1542cdc442be",  # pragma: allowlist secret
            "provider": {},
            "areaHa": 5081.838068566336,
            "bbox": [-6.031685272, 36.160220287, -5.879245686, 36.249656987],
            "lock": False,
            "info": {"use": {}, "wdpaid": 142809},
        },
    }
}


@pytest.mark.asyncio
async def test_wdpa_geostore_mock_helper(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    async def mock_get_geostore_by_wdpa_id(wdpa_id, x_api_key):
        return RWGeostoreResponse(**example_geostore_resp)

    monkeypatch.setattr(
        geostore, "get_geostore_by_wdpa_id", mock_get_geostore_by_wdpa_id
    )

    response = await async_client.get("/geostore/wdpa/142809")

    assert response.json() == example_geostore_resp
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_wdpa_geostore_passes_through(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    async def mock_resp_func(request: Request) -> Response:
        return Response(status_code=200, json=example_geostore_resp)

    transport = MockTransport(mock_resp_func)

    mocked_client = partial(AsyncClient, transport=transport)
    monkeypatch.setattr(rw_api, "AsyncClient", mocked_client)
    response = await async_client.get("/geostore/wdpa/142809")

    assert response.json() == example_geostore_resp
    assert response.status_code == 200
