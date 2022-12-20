from unittest.mock import patch

import pytest

from app.tasks.raster_tile_cache_assets import raster_tile_cache_asset


@patch(
    "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.execute",
    autospec=True,
)
@patch(
    "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.symbology_constructor",
    autospec=True,
)
@patch(
    "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.reproject_to_web_mercator",
    autospec=True,
)
@patch(
    "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.get_asset",
    autospec=True,
)
class TestCrudCollaboration:
    @pytest.mark.asyncio
    async def test_source_asset_is_retrieved_by_uuid(
        self,
        get_asset_mock,
        web_mercator_dummy,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        change_log,
    ):
        get_asset_mock.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        get_asset_mock.assert_called_with(
            creation_options_dict["creation_options"]["source_asset_id"]
        )
