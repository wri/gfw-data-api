from functools import partial
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch
from httpx import AsyncClient, MockTransport, Request, Response
from starlette.datastructures import QueryParams

from app.crud import geostore as crud_geostore
from app.models.pydantic.geostore import (
    AdminGeostoreResponse,
    AdminListResponse,
    Geostore,
    GeostoreResponse,
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

real_sample_geostore_resp = {
    "status": "success",
    "data": {
        "created_on": "2024-06-21T23:29:15.799130",
        "updated_on": "2024-06-21T23:29:15.799136",
        "gfw_geostore_id": "b9faa657-34c9-96d4-fce4-8bb8a1507cb3",
        "gfw_geojson": {
            "type": "MultiPolygon",
            "coordinates": [
                [
                    [
                        [10.67647934, 53.857791641],
                        [10.699653625, 53.857791641],
                        [10.699653625, 53.875758665],
                        [10.67647934, 53.875758665],
                        [10.67647934, 53.857791641],
                    ]
                ]
            ],
        },
        "gfw_area__ha": 304.86964509449007,
        "gfw_bbox": [10.67647934, 53.857791641, 10.699653625, 53.875758665],
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
async def test_get_gadm_geostore_by_country_with_gadm_36_calls_rw_branch_with_correct_args(
    apikey,
    async_client: AsyncClient,
):
    country = "MEX"
    url = f"/geostore/admin/{country}"
    params = {"source[version]": "3.6"}

    with patch(
        "app.routes.geostore.geostore.rw_get_boundary_by_country_id",
        return_value=AdminGeostoreResponse(**example_geostore_resp),
    ) as mock_rw_get_boundary_by_country_id:
        resp = await async_client.get(
            url, params=params, headers={"x-api-key": apikey[0]}
        )

    assert resp.status_code == 200
    assert mock_rw_get_boundary_by_country_id.called is True
    assert mock_rw_get_boundary_by_country_id.call_args.args == (
        country,
        QueryParams(params),
    )


@pytest.mark.asyncio
async def test_get_gadm_geostore_by_country_with_gadm_41_calls_gfw_branch_with_correct_args(
    apikey,
    async_client: AsyncClient,
):
    country = "MEX"
    url = f"/geostore/admin/{country}"
    params = {"source[version]": "4.1"}

    with patch(
        "app.routes.geostore.geostore.geostore.get_gadm_geostore"
    ) as mock_get_gadm_geostore:
        mock_get_gadm_geostore.return_value = AdminGeostoreResponse(
            **example_geostore_resp
        )
        resp = await async_client.get(
            url, params=params, headers={"x-api-key": apikey[0]}
        )

    assert resp.status_code == 200
    assert mock_get_gadm_geostore.called is True
    assert mock_get_gadm_geostore.call_args.args == ("gadm", "4.1", 0, None, country)


@pytest.mark.asyncio
async def test_get_gadm_geostore_by_region_with_gadm_41_calls_gfw_branch_with_correct_args(
    apikey,
    async_client: AsyncClient,
):
    country = "MEX"
    region = "2"
    url = f"/geostore/admin/{country}/{region}"
    params = {"source[version]": "4.1"}

    with patch(
        "app.routes.geostore.geostore.geostore.get_gadm_geostore"
    ) as mock_get_gadm_geostore:
        mock_get_gadm_geostore.return_value = AdminGeostoreResponse(
            **example_geostore_resp
        )
        _ = await async_client.get(url, params=params, headers={"x-api-key": apikey[0]})

    assert mock_get_gadm_geostore.called is True
    assert mock_get_gadm_geostore.call_args.args == (
        "gadm",
        "4.1",
        1,
        None,
        country,
        region,
    )


@pytest.mark.asyncio
async def test_get_gadm_geostore_by_subregion_with_gadm_41_calls_gfw_branch_with_correct_args(
    apikey,
    async_client: AsyncClient,
):
    country = "MEX"
    region = "2"
    subregion = "1"
    url = f"/geostore/admin/{country}/{region}/{subregion}"
    params = {"source[version]": "4.1"}

    with patch(
        "app.routes.geostore.geostore.geostore.get_gadm_geostore"
    ) as mock_get_gadm_geostore:
        mock_get_gadm_geostore.return_value = AdminGeostoreResponse(
            **example_geostore_resp
        )
        _ = await async_client.get(url, params=params, headers={"x-api-key": apikey[0]})

    assert mock_get_gadm_geostore.called is True
    assert mock_get_gadm_geostore.call_args.args == (
        "gadm",
        "4.1",
        2,
        None,
        country,
        region,
        subregion,
    )


@pytest.mark.asyncio
async def test_get_admin_sub_region_geostore(
    apikey, async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    async def mock_resp_func(request: Request) -> Response:
        return Response(status_code=200, json=example_admin_geostore_snipped)

    transport = MockTransport(mock_resp_func)

    mocked_client = partial(AsyncClient, transport=transport)
    monkeypatch.setattr(rw_api, "AsyncClient", mocked_client)
    response = await async_client.get(
        "/geostore/admin/MEX/1/1", headers={"x-api-key": apikey[0]}
    )

    assert response.json() == example_admin_geostore_snipped
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_admin_geostore_with_query_params(
    apikey, async_client: AsyncClient, monkeypatch: MonkeyPatch
):
    async def mock_resp_func(request: Request) -> Response:
        assert "foo" in str(request.url)
        return Response(status_code=200, json=example_admin_geostore_snipped)

    transport = MockTransport(mock_resp_func)

    mocked_client = partial(AsyncClient, transport=transport)
    monkeypatch.setattr(rw_api, "AsyncClient", mocked_client)
    response = await async_client.get(
        "/geostore/admin/MEX/1/1?foo=bar", headers={"x-api-key": apikey[0]}
    )

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
async def test_get_geostore_by_rw_style_id_proxies_to_rw(async_client: AsyncClient):
    url = "/geostore/88db597b6bcd096fb80d1542cdc442be"

    with patch(
        "app.routes.geostore.geostore.geostore.get_gfw_geostore_from_any_dataset",
        side_effect=Exception,
    ) as mock_get_gfw_geostore_from_any_dataset:
        with patch(
            "app.routes.geostore.geostore.proxy_get_geostore",
            return_value=GeostoreResponse.parse_obj(real_sample_geostore_resp),
        ) as mock_proxy_get_geostore:
            resp = await async_client.get(url)

    assert resp.status_code == 200
    assert mock_proxy_get_geostore.called is True
    assert mock_get_gfw_geostore_from_any_dataset.called is False


@pytest.mark.asyncio
async def test_get_geostore_by_gfw_style_id_queries_data_api(async_client: AsyncClient):
    url = "/geostore/b9faa657-34c9-96d4-fce4-8bb8a1507cb3"

    with patch(
        "app.routes.geostore.geostore.geostore.get_gfw_geostore_from_any_dataset",
        return_value=Geostore.parse_obj(real_sample_geostore_resp["data"]),
    ) as mock_get_gfw_geostore_from_any_dataset:
        with patch(
            "app.routes.geostore.geostore.proxy_get_geostore", side_effect=Exception
        ) as mock_proxy_get_geostore:
            resp = await async_client.get(url)

    assert resp.status_code == 200
    assert mock_proxy_get_geostore.called is False
    assert mock_get_gfw_geostore_from_any_dataset.called is True


@pytest.mark.asyncio
async def test_get_admin_list_proxies_to_rw_when_no_source_info_is_provided(
    apikey, async_client: AsyncClient
):
    url = "/geostore/admin/list"

    with patch(
        "app.routes.geostore.geostore.rw_get_admin_list"
    ) as mock_rw_get_admin_list:
        mock_rw_get_admin_list.return_value = AdminListResponse(**example_admin_list)
        resp = await async_client.get(url, headers={"x-api-key": apikey[0]})

    assert mock_rw_get_admin_list.called is True
    assert resp.json().get("status") == "success"


@pytest.mark.asyncio
async def test_get_admin_list_proxies_to_rw_when_gadm_36_requested(
    apikey, async_client: AsyncClient
):
    url = "/geostore/admin/list"
    params = {"source[version]": "3.6"}

    with patch(
        "app.routes.geostore.geostore.rw_get_admin_list"
    ) as mock_rw_get_admin_list:
        mock_rw_get_admin_list.return_value = AdminListResponse(**example_admin_list)
        resp = await async_client.get(
            url, params=params, headers={"x-api-key": apikey[0]}
        )

    assert mock_rw_get_admin_list.called is True
    assert resp.json().get("status") == "success"


@pytest.mark.asyncio
async def test_get_admin_list_gets_gadm_41_from_data_api(
    apikey, async_client: AsyncClient
):
    url = "/geostore/admin/list"
    params = {"source[version]": "4.1"}

    with patch(
        "app.routes.geostore.geostore.geostore.get_admin_boundary_list"
    ) as mock_gfw_get_admin_list:
        mock_gfw_get_admin_list.return_value = AdminListResponse(**example_admin_list)
        resp = await async_client.get(
            url, params=params, headers={"x-api-key": apikey[0]}
        )

    assert resp.json().get("status") == "success"
    assert mock_gfw_get_admin_list.called is True
