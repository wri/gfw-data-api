from functools import partial
from unittest.mock import AsyncMock, MagicMock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient, MockTransport, Request, Response

from app.crud import geostore as crud_geostore
from app.models.pydantic.geostore import (
    AdminGeostoreResponse,
    AdminListResponse,
    Geostore,
)
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
        },
    ]
}


example_admin_geostore_snipped = {
    "status": "success",
    "data": {
        "type": "geoStore",
        "id": "851679102625f53c3254df99efbfba17",  # pragma: allowlist secret
        "attributes": {
            "geojson": {
                "crs": {},
                "features": [
                    {
                        "geometry": {
                            "coordinates": [
                                [
                                    [
                                        [-102.44799041748, 21.6610717773437],
                                        [-102.455261230469, 21.6625995635987],
                                        [-102.460227966309, 21.6666717529297],
                                        [-102.443138122558, 21.6583995819092],
                                        [-102.44799041748, 21.6610717773437],
                                        [-102.44799041748, 21.6610717773437],
                                    ]
                                ]
                            ],
                            "type": "MultiPolygon",
                        },
                        "properties": None,
                        "type": "Feature",
                    }
                ],
                "type": "FeatureCollection",
            },
            "hash": "851679102625f53c3254df99efbfba17",  # pragma: allowlist secret
            "provider": {},
            "areaHa": 168384.12865462166,
            "bbox": [
                -102.61302947998,
                21.6217498779298,
                -101.896156311035,
                22.0676307678222,
            ],
            "lock": False,
            "info": {
                "use": {},
                "simplifyThresh": 0.00005,
                "gadm": "3.6",
                "name": "Aguascalientes",
                "id2": 1,
                "id1": 1,
                "iso": "MEX",
            },
        },
    },
}


example_geostore_resp = {
    "status": "success",
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
    },
}


create_rw_geostore_payload = {
    "geojson": {
        "type": "MultiPolygon",
        "coordinates": [[[[8, 51], [11, 55], [12, 49], [8, 52]]]],
    }
}


create_gfw_geostore_payload = {
    "geometry": {
        "type": "MultiPolygon",
        "coordinates": [[[[8, 51], [11, 55], [12, 49], [8, 52]]]],
    }
}

create_rw_geostore_response = {
    "data": {
        "type": "geoStore",
        "id": "b73a4b48eb305110b8bfa604fe58df82",  # pragma: allowlist secret
        "attributes": {
            "geojson": {
                "crs": {},
                "type": "FeatureCollection",
                "features": [
                    {
                        "geometry": {
                            "coordinates": [[[[8, 51], [11, 55], [12, 49], [8, 51]]]],
                            "type": "MultiPolygon",
                        },
                        "type": "Feature",
                        "properties": None,
                    }
                ],
            },
            "hash": "b73a4b48eb305110b8bfa604fe58df82",  # pragma: allowlist secret
            "provider": {},
            "areaHa": 8354467.362218728,
            "bbox": [8, 49, 12, 55],
            "lock": False,
            "info": {"use": {}},
        },
    }
}


create_gfw_geostore_data = {
    "created_on": "2025-01-21T15:24:10.315976",
    "updated_on": "2025-01-21T15:24:10.315986",
    "gfw_geostore_id": "db2b4428-bad2-fc94-1ea8-041597dc482c",
    "gfw_geojson": {
        "type": "MultiPolygon",
        "coordinates": [[[[8, 51], [11, 55], [12, 49], [8, 52]]]],
    },
    "gfw_area__ha": 8640517.2263933,
    "gfw_bbox": [8.0, 49.0, 12.0, 55.0],
}


@pytest.mark.asyncio
async def test_get_admin_country_geostore(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    url = "/geostore/admin/MEX"
    params = {"adminVersion": "4.1"}

    response = await async_client.get(url, params=params)

    assert response.json() == example_admin_geostore_snipped
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_admin_sub_region_geostore(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    async def mock_resp_func(request: Request) -> Response:
        return Response(status_code=200, json=example_admin_geostore_snipped)

    transport = MockTransport(mock_resp_func)

    mocked_client = partial(AsyncClient, transport=transport)
    monkeypatch.setattr(rw_api, "AsyncClient", mocked_client)
    response = await async_client.get("/geostore/admin/MEX/1/1")

    assert response.json() == example_admin_geostore_snipped
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_admin_geostore_with_query_params(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    async def mock_resp_func(request: Request) -> Response:
        assert "foo" in str(request.url)
        return Response(status_code=200, json=example_admin_geostore_snipped)

    transport = MockTransport(mock_resp_func)

    mocked_client = partial(AsyncClient, transport=transport)
    monkeypatch.setattr(rw_api, "AsyncClient", mocked_client)
    response = await async_client.get("/geostore/admin/MEX/1/1?foo=bar")

    assert response.json() == example_admin_geostore_snipped
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_add_geostore_rw_branch(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    url = "/geostore"

    mock_create_rw_geostore = AsyncMock(
        return_value=AdminGeostoreResponse(**create_rw_geostore_response),
        spec=geostore.create_rw_geostore,
    )
    monkeypatch.setattr(geostore, "create_rw_geostore", mock_create_rw_geostore)

    mock_geostore_obj = MagicMock(spec=Geostore)
    mock_create_gfw_geostore = AsyncMock(
        return_value=mock_geostore_obj, spec=crud_geostore.create_user_area
    )
    monkeypatch.setattr(crud_geostore, "create_user_area", mock_create_gfw_geostore)

    _ = await async_client.post(
        url, json=create_rw_geostore_payload, follow_redirects=True
    )

    assert mock_create_rw_geostore.called is True
    assert mock_create_gfw_geostore.called is False


@pytest.mark.asyncio
async def test_add_geostore_gfw_branch(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    url = "/geostore"

    mock_create_rw_geostore = AsyncMock(
        return_value=AdminGeostoreResponse(**create_rw_geostore_response),
        spec=geostore.create_rw_geostore,
    )
    monkeypatch.setattr(geostore, "create_rw_geostore", mock_create_rw_geostore)

    mock_create_gfw_geostore = AsyncMock(
        return_value=create_gfw_geostore_data, spec=crud_geostore.create_user_area
    )
    monkeypatch.setattr(crud_geostore, "create_user_area", mock_create_gfw_geostore)

    _ = await async_client.post(
        url, json=create_gfw_geostore_payload, follow_redirects=True
    )

    assert mock_create_rw_geostore.called is False
    assert mock_create_gfw_geostore.called is True


@pytest.mark.asyncio
async def test_get_geostore_rw_branch(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    url = "/geostore/88db597b6bcd096fb80d1542cdc442be"

    mock_proxy_get_geostore = AsyncMock(
        return_value=AdminGeostoreResponse(**example_geostore_resp),
        spec=geostore.proxy_get_geostore,
    )
    monkeypatch.setattr(geostore, "proxy_get_geostore", mock_proxy_get_geostore)

    mock_geostore_obj = MagicMock(spec=Geostore)
    mock_get_gfw_geostore_from_any_dataset = AsyncMock(
        return_value=mock_geostore_obj,
        spec=crud_geostore.get_gfw_geostore_from_any_dataset,
    )
    monkeypatch.setattr(
        crud_geostore,
        "get_gfw_geostore_from_any_dataset",
        mock_get_gfw_geostore_from_any_dataset,
    )

    _ = await async_client.get(url)

    assert mock_proxy_get_geostore.called is True
    assert mock_get_gfw_geostore_from_any_dataset.called is False


@pytest.mark.asyncio
async def test_get_geostore_gfw_branch(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    url = "/geostore/db2b4428-bad2-fc94-1ea8-041597dc482c"

    mock_proxy_get_geostore = AsyncMock(
        return_value=AdminGeostoreResponse(**example_geostore_resp),
        spec=geostore.proxy_get_geostore,
    )
    monkeypatch.setattr(geostore, "proxy_get_geostore", mock_proxy_get_geostore)

    mock_get_gfw_geostore_from_any_dataset = AsyncMock(
        return_value=Geostore(**create_gfw_geostore_data),
        spec=crud_geostore.get_gfw_geostore_from_any_dataset,
    )
    monkeypatch.setattr(
        crud_geostore,
        "get_gfw_geostore_from_any_dataset",
        mock_get_gfw_geostore_from_any_dataset,
    )

    _ = await async_client.get(url)

    assert mock_proxy_get_geostore.called is False
    assert mock_get_gfw_geostore_from_any_dataset.called is True


@pytest.mark.asyncio
async def test_get_admin_list_rw_branch(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    url = "/geostore/admin/list"

    mock_rw_get_admin_list = AsyncMock(
        return_value=AdminListResponse(**example_admin_list),
        spec=rw_api.rw_get_admin_list,
    )
    monkeypatch.setattr(geostore, "rw_get_admin_list", mock_rw_get_admin_list)

    _ = await async_client.get(url)

    assert mock_rw_get_admin_list.called is True


@pytest.mark.asyncio
async def test_get_admin_list_rw_branch_36(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    url = "/geostore/admin/list"
    params = {"adminVersion": "3.6"}

    mock_rw_get_admin_list = AsyncMock(
        return_value=AdminListResponse(**example_admin_list),
        spec=rw_api.rw_get_admin_list,
    )
    monkeypatch.setattr(geostore, "rw_get_admin_list", mock_rw_get_admin_list)

    resp = await async_client.get(url, params=params)

    assert mock_rw_get_admin_list.called is True
    assert resp.json().get("status") == "success"


@pytest.mark.asyncio
async def test_get_admin_list_gfw_branch_41(
    async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    url = "/geostore/admin/list"
    params = {"source[version]": "4.1"}

    mock_gfw_get_admin_list = AsyncMock(
        return_value=AdminListResponse(**example_admin_list),
        spec=crud_geostore.get_admin_boundary_list,
    )
    monkeypatch.setattr(
        crud_geostore, "get_admin_boundary_list", mock_gfw_get_admin_list
    )

    resp = await async_client.get(url, params=params)

    assert mock_gfw_get_admin_list.called is True
    assert resp.json().get("status") == "success"
