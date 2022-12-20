from unittest.mock import patch

import pytest

from app.tasks.raster_tile_cache_assets import raster_tile_cache_asset

from . import MODULE_PATH_UNDER_TEST


@patch(f"{MODULE_PATH_UNDER_TEST}.execute", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.symbology_constructor", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.reproject_to_web_mercator", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.get_asset", autospec=True)
class TestWebMercatorReProjectionCollaboration:
    @pytest.mark.asyncio
    async def test_is_called_with_dataset_and_version(
        self,
        get_asset_dummy,
        web_mercator_mock,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_mock.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = web_mercator_mock.call_args_list[-1]
        assert args[:2] == (
            "test_dataset",
            "2022",
        ), "`dataset` and `version` do not match"

    @pytest.mark.asyncio
    async def test_is_called_with_asset_creation_options(
        self,
        get_asset_dummy,
        web_mercator_mock,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        change_log,
        raster_tile_set_source_creation_options,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_mock.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = web_mercator_mock.call_args_list[-1]
        assert args[2].pixel_meaning == "test_pixels_default"

    @pytest.mark.asyncio
    async def test_is_called_same_max_zoom_level_as_passed_in_creation_options(
        self,
        get_asset_dummy,
        web_mercator_mock,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_mock.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = web_mercator_mock.call_args_list[-1]
        assert args[3:5] == (
            0,
            0,
        ), "`zoom_level` and `max_zoom` values were not as expected"

    @pytest.mark.asyncio
    async def test_is_called_with_no_source_reprojection_parent_jobs(
        self,
        get_asset_dummy,
        web_mercator_mock,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_mock.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = web_mercator_mock.call_args_list[-1]
        assert args[5] == []

    @pytest.mark.asyncio
    async def test_is_called_with_resampling_kwargs(
        self,
        get_asset_dummy,
        web_mercator_mock,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_mock.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        _, kwargs = web_mercator_mock.call_args_list[-1]
        assert kwargs == {
            "max_zoom_resampling": "nearest",
            "max_zoom_calc": None,
            "use_resampler": True,
        }, "`Resampling` arguments do not match"
