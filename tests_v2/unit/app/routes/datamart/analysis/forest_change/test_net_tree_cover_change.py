from unittest.mock import AsyncMock, patch, ANY
from fastapi import status
import pytest
from httpx import AsyncClient

from app.routes.datamart.analysis.forest_change.tree_cover_change import TreeCoverData

# Define the stubbed response for _fetch_tree_cover_data so the route is successful
stubbed_tree_cover_data = TreeCoverData(
    iso="BRA",
    adm1=12,
    adm2=34,
    stable=413722809.3,
    loss=36141245.77,
    gain=8062324.946,
    disturb=23421628.86,
    net=-28078920.83,
    change=-5.932759761810303,
    gfw_area__ha=850036547.481532
)


# Common helper function to send the request
async def send_tree_cover_change_request(api_key, async_client: AsyncClient, params):
    headers = {"x-api-key": api_key}
    response = await async_client.get(
        f"/datamart/analysis/forest_change/tree_cover_change/net_tree_cover_change",
        params=params,
        headers=headers,
        follow_redirects=True,
    )
    return response


@pytest.mark.asyncio
async def test_net_tree_cover_change_builds_succeeds(
        apikey, async_client: AsyncClient
):
    with patch(
            "app.routes.datamart.analysis.forest_change.tree_cover_change._fetch_tree_cover_data",
            AsyncMock(return_value=stubbed_tree_cover_data)
    ):
        api_key, payload = apikey
        params = {"iso": "BRA"}

        response = await send_tree_cover_change_request(api_key, async_client, params)

        assert response.status_code == status.HTTP_200_OK, "Expected status code 200 OK"


@pytest.mark.asyncio
class TestSQLBuilding:
    @pytest.mark.asyncio
    async def test_net_tree_cover_change_builds_sql_with_iso(self, apikey, async_client: AsyncClient):
        with patch(
                "app.routes.datamart.analysis.forest_change.tree_cover_change._fetch_tree_cover_data",
                AsyncMock(return_value=stubbed_tree_cover_data)
        ) as mock_fetch:
            api_key, payload = apikey
            params = {"iso": "BRA"}

            await send_tree_cover_change_request(api_key, async_client, params)

            mock_fetch.assert_called_once_with(
                "SELECT iso, stable, loss, gain, disturb, net, change, gfw_area__ha FROM data WHERE iso = 'BRA'",
                # SQL query
                ANY,  # Ignore admin level
                ANY  # Ignore API Key
            )


    @pytest.mark.asyncio
    async def test_net_tree_cover_change_builds_sql_with_adm1(self, apikey, async_client: AsyncClient):
        with patch(
                "app.routes.datamart.analysis.forest_change.tree_cover_change._fetch_tree_cover_data",
                AsyncMock(return_value=stubbed_tree_cover_data)
        ) as mock_fetch:
            api_key, payload = apikey
            params = {"iso": "BRA", "adm1": 12}

            await send_tree_cover_change_request(api_key, async_client, params)

            mock_fetch.assert_called_once_with(
                "SELECT iso, adm1, stable, loss, gain, disturb, net, change, gfw_area__ha FROM data WHERE iso = 'BRA' AND adm1 = '12'",
                # SQL query
                ANY,  # Ignore admin level
                ANY  # Ignore API Key
            )


    @pytest.mark.asyncio
    async def test_net_tree_cover_change_builds_sql_with_adm2(self, apikey, async_client: AsyncClient):
        with patch(
                "app.routes.datamart.analysis.forest_change.tree_cover_change._fetch_tree_cover_data",
                AsyncMock(return_value=stubbed_tree_cover_data)
        ) as mock_fetch:
            api_key, payload = apikey
            params = {"iso": "BRA", "adm1": 12, "adm2": 34}

            await send_tree_cover_change_request(api_key, async_client, params)

            mock_fetch.assert_called_once_with(
                "SELECT iso, adm1, adm2, stable, loss, gain, disturb, net, change, gfw_area__ha FROM data WHERE iso = 'BRA' AND adm1 = '12' AND adm2 = '34'",
                # SQL query
                ANY,  # Ignore admin level
                ANY  # Ignore API Key
            )


@pytest.mark.asyncio
class TestAdminLevel:
    @pytest.mark.asyncio
    async def test_net_tree_cover_change_passes_iso(self, apikey, async_client):
        api_key, payload = apikey
        with patch(
                "app.routes.datamart.analysis.forest_change.tree_cover_change._fetch_tree_cover_data",
                AsyncMock(return_value=stubbed_tree_cover_data)
        ) as mock_fetch:
            params = {"iso": "BRA"}

            await send_tree_cover_change_request(api_key, async_client, params)

            mock_fetch.assert_called_once_with(
                ANY,  # Ignore SQL
                'adm0',  # most precise adm level
                ANY  # Ignore API Key
            )

    @pytest.mark.asyncio
    async def test_net_tree_cover_change_passes_adm1(self, apikey, async_client):
        api_key, payload = apikey
        with patch(
                "app.routes.datamart.analysis.forest_change.tree_cover_change._fetch_tree_cover_data",
                AsyncMock(return_value=stubbed_tree_cover_data)
        ) as mock_fetch:
            params = {"iso": "BRA", "adm1": "12"}

            await send_tree_cover_change_request(api_key, async_client, params)

            mock_fetch.assert_called_once_with(
                ANY,  # Ignore SQL
                'adm1',  # most precise adm level
                ANY  # Ignore API Key
            )

    @pytest.mark.asyncio
    async def test_net_tree_cover_change_passes_adm2(self, apikey, async_client):
        api_key, payload = apikey
        with patch(
                "app.routes.datamart.analysis.forest_change.tree_cover_change._fetch_tree_cover_data",
                AsyncMock(return_value=stubbed_tree_cover_data)
        ) as mock_fetch:
            params = {"iso": "BRA", "adm1": "12", "adm2": "34"}

            await send_tree_cover_change_request(api_key, async_client, params)

            mock_fetch.assert_called_once_with(
                ANY,  # Ignore SQL
                'adm2',  # most precise adm level
                ANY  # Ignore API Key
            )


@pytest.mark.asyncio
async def test_net_tree_cover_change_passes_api_key(
        apikey, async_client: AsyncClient
):
    with patch(
            "app.routes.datamart.analysis.forest_change.tree_cover_change._fetch_tree_cover_data",
            AsyncMock(return_value=stubbed_tree_cover_data)
    ) as mock_fetch:
        api_key, payload = apikey
        params = {"iso": "BRA"}

        await send_tree_cover_change_request(api_key, async_client, params)

        mock_fetch.assert_called_once_with(
            ANY,  # Ignore SQL
            ANY,  # Ignore admin level
            api_key  # api key
        )
