from uuid import UUID

import pytest
import respx
from httpx import Response

from app.errors import InvalidResponseError, RecordNotFoundError
from app.models.pydantic.geostore import GeostoreCommon
from app.settings.globals import RW_API_URL
from app.utils import rw_api
from tests_v2.fixtures.sample_rw_geostore_response import response_body


@pytest.mark.asyncio
async def test_get_geostore_success():
    # Just to make sure we're not actually hitting the RW API,
    # ask for a non-existent geostore
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a5"
    geostore_id_uuid = UUID(geostore_id_str)

    with respx.mock:
        rw_geostore_route = respx.get(f"{RW_API_URL}/v2/geostore/{geostore_id_str}")
        rw_geostore_route.return_value = Response(200, json=response_body)

        geo: GeostoreCommon = await rw_api.get_geostore(geostore_id_uuid)

        # Compare against the id actually in the response fixture, above,
        # which has the payload of a valid geostore
        assert geo.geostore_id == UUID("d8907d30eb5ec7e33a68aa31aaf918a4")


@pytest.mark.asyncio
async def test_get_geostore_404():
    # Just to make sure we're not actually hitting the RW API,
    # ask for a valid geostore
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a4"
    geostore_id_uuid = UUID(geostore_id_str)

    with respx.mock:
        rw_geostore_route = respx.get(f"{RW_API_URL}/v2/geostore/{geostore_id_str}")
        rw_geostore_route.return_value = Response(
            404, json={"errors": [{"status": 404, "detail": "GeoStore not found"}]}
        )

        with pytest.raises(RecordNotFoundError):
            _ = await rw_api.get_geostore(geostore_id_uuid)


@pytest.mark.asyncio
async def test_get_geostore_other():
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a6"
    geostore_id_uuid = UUID(geostore_id_str)

    with respx.mock:
        rw_geostore_route = respx.get(f"{RW_API_URL}/v2/geostore/{geostore_id_str}")
        rw_geostore_route.return_value = Response(
            666, json={"errors": [{"status": 666, "detail": "Bad stuff"}]}
        )

        with pytest.raises(InvalidResponseError):
            _ = await rw_api.get_geostore(geostore_id_uuid)
