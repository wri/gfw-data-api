from uuid import UUID

import pytest
import respx
from httpx import Response

from app.errors import InvalidResponseError, RecordNotFoundError
from app.models.pydantic.geostore import GeostoreCommon
from app.settings.globals import RW_API_URL
from app.utils.rw_api import get_geostore

rw_api_geostore_json = {
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


@pytest.mark.asyncio
async def test_get_geostore_legacy_success():
    # Just to make sure we're not actually hitting the RW API,
    # ask for a non-existent geostore
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a5"
    geostore_id_uuid = UUID(geostore_id_str)

    with respx.mock:
        rw_geostore_route = respx.get(f"{RW_API_URL}/v2/geostore/{geostore_id_str}")
        rw_geostore_route.return_value = Response(200, json=rw_api_geostore_json)

        geo: GeostoreCommon = await get_geostore(geostore_id_uuid)

        # Compare against the id actually in the response fixture, above,
        # which has the payload of a valid geostore
        assert geo.geostore_id == UUID("d8907d30eb5ec7e33a68aa31aaf918a4")


@pytest.mark.asyncio
async def test_get_geostore_legacy_404():
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
            _ = await get_geostore(geostore_id_uuid)


@pytest.mark.asyncio
async def test_get_geostore_legacy_other():
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a6"
    geostore_id_uuid = UUID(geostore_id_str)

    with respx.mock:
        rw_geostore_route = respx.get(f"{RW_API_URL}/v2/geostore/{geostore_id_str}")
        rw_geostore_route.return_value = Response(
            666, json={"errors": [{"status": 666, "detail": "Bad stuff"}]}
        )

        with pytest.raises(InvalidResponseError):
            _ = await get_geostore(geostore_id_uuid)
