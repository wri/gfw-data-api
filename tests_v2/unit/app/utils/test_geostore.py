from unittest.mock import MagicMock
from uuid import UUID

import pytest
import respx
from _pytest.monkeypatch import MonkeyPatch

from app.models.enum.geostore import GeostoreOrigin
from app.settings import globals
from app.utils import geostore

# from httpx import Response


@pytest.mark.asyncio
async def test_get_geostore_default_is_legacy(monkeypatch: MonkeyPatch):
    geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a7"
    geostore_id_uuid = UUID(geostore_id_str)

    # First test that the default is to use the legacy behavior
    mock_get_geostore_legacy = MagicMock(geostore.get_geostore_legacy)
    monkeypatch.setattr(geostore, "get_geostore_legacy", mock_get_geostore_legacy)

    assert mock_get_geostore_legacy.called is False

    with respx.mock:
        _ = respx.get(f"{globals.RW_API_URL}/v2/geostore/{geostore_id_str}")
        _ = await geostore.get_geostore(
            geostore_id_uuid, geostore_origin=GeostoreOrigin.rw
        )

    assert mock_get_geostore_legacy.called is True


# @pytest.mark.asyncio
# async def test_get_geostore_flag_triggers_new_behavior():
#     geostore_id_str = "d8907d30eb5ec7e33a68aa31aaf918a7"
#     geostore_id_uuid = UUID(geostore_id_str)
#
#     # Now test that setting the env var triggers calling the new function
#     with MonkeyPatch.context() as monkeypatch:
#         monkeypatch.setattr(globals, "FEATURE_CHECK_ALL_GEOSTORES", "TRUE")
#
#         mock_get_geostore_from_any_source = MagicMock(geostore.get_geostore_from_any_source)
#         monkeypatch.setattr(geostore, "get_geostore_from_any_source", mock_get_geostore_from_any_source)
#
#         assert mock_get_geostore_from_any_source.called is False
#
#         with respx.mock:
#             rw_geostore_route = respx.get(f"{globals.RW_API_URL}/v2/geostore/{geostore_id_str}")
#             rw_geostore_route.return_value = Response(
#                 404, json={"errors": [{"status": 404, "detail": "GeoStore not found"}]}
#             )
#
#             try:
#                 _ = await geostore.get_geostore(geostore_id_uuid, geostore_origin=GeostoreOrigin.rw)
#             except:
#                 pass
#         assert mock_get_geostore_from_any_source.called is True
