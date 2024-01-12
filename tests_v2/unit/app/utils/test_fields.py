from unittest.mock import AsyncMock

import pytest
from _pytest.monkeypatch import MonkeyPatch

from app.crud.assets import get_default_asset
from app.models.pydantic.creation_options import StaticVector1x1CreationOptions
from app.utils import fields


@pytest.mark.asyncio
async def test_get_field_attributes_no_specified_fields(monkeypatch: MonkeyPatch):
    creation_options = {}

    mock_get_default_asset = AsyncMock(get_default_asset)
    monkeypatch.setattr(fields, "get_default_asset", mock_get_default_asset)

    mock_get_asset_fields_dicts = AsyncMock(get_default_asset)
    mock_get_asset_fields_dicts.return_value = [
        {"name": "something_wacky", "is_feature_info": True},
        {"name": "gid_2", "is_feature_info": True},
        {"name": "not_feature_field", "is_feature_info": False},
        {"name": "something_else", "is_feature_info": True},
    ]
    monkeypatch.setattr(fields, "get_asset_fields_dicts", mock_get_asset_fields_dicts)

    foo = await fields.get_field_attributes("some_dataset", "v1.5", StaticVector1x1CreationOptions(**creation_options))
    assert foo == [
        {"name": "something_wacky", "is_feature_info": True},
        {"name": "gid_2", "is_feature_info": True},
        {"name": "something_else", "is_feature_info": True},
    ]


@pytest.mark.asyncio
async def test_get_field_attributes_respects_requested_order_1(monkeypatch: MonkeyPatch):
    creation_options = {
        "include_tile_id": True,
        "field_attributes": [
            "gfw_geostore_id",
            "gid_0",
            "gid_1",
            "gid_2"
        ]
    }

    mock_get_default_asset = AsyncMock(get_default_asset)
    monkeypatch.setattr(fields, "get_default_asset", mock_get_default_asset)

    mock_get_asset_fields_dicts = AsyncMock(get_default_asset)
    mock_get_asset_fields_dicts.return_value = [
        {"name": "something_wacky", "is_feature_info": True},
        {"name": "gid_2", "is_feature_info": True},
        {"name": "gid_0", "is_feature_info": True},
    ]
    monkeypatch.setattr(fields, "get_asset_fields_dicts", mock_get_asset_fields_dicts)

    foo = await fields.get_field_attributes("some_dataset", "v1.5", StaticVector1x1CreationOptions(**creation_options))
    assert foo == [
        {"name": "gid_0", "is_feature_info": True},
        {"name": "gid_2", "is_feature_info": True},
    ]


@pytest.mark.asyncio
async def test_get_field_attributes_respects_requested_order_2(monkeypatch: MonkeyPatch):
    creation_options = {
        "include_tile_id": True,
        "field_attributes": [
            "gfw_geostore_id",
            "gid_0",
            "gid_1",
            "gid_2"
        ]
    }

    mock_get_default_asset = AsyncMock(get_default_asset)
    monkeypatch.setattr(fields, "get_default_asset", mock_get_default_asset)

    mock_get_asset_fields_dicts = AsyncMock(get_default_asset)
    mock_get_asset_fields_dicts.return_value = [
        {"name": "something_wacky", "is_feature_info": True},
        {"name": "gid_2", "is_feature_info": True},
        {"name": "gid_0", "is_feature_info": True},
    ]
    monkeypatch.setattr(fields, "get_asset_fields_dicts", mock_get_asset_fields_dicts)

    foo = await fields.get_field_attributes("some_dataset", "v1.5", StaticVector1x1CreationOptions(**creation_options))
    assert foo == [
        {"name": "gid_0", "is_feature_info": True},
        {"name": "gid_2", "is_feature_info": True},
    ]
