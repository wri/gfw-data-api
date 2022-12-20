from unittest.mock import patch

import pytest

from app.tasks.raster_tile_cache_assets import raster_tile_cache_asset

from . import MODULE_PATH_UNDER_TEST


@patch(f"{MODULE_PATH_UNDER_TEST}.execute", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.symbology_constructor", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.reproject_to_web_mercator", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.get_asset", autospec=True)
class TestSymbologyFunctionCollaboration:
    @pytest.mark.asyncio
    async def test_is_called_with_dataset_and_version(
        self,
        get_asset_dummy,
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
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = symbology_info.function.call_args_list[-1]

        assert args[:2] == ("test_dataset", "2022")

    @pytest.mark.asyncio
    async def test_is_called_the_implementation_from_the_input_creation_options(
        self,
        get_asset_dummy,
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
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = symbology_info.function.call_args_list[-1]

        assert args[2] == "default"

    @pytest.mark.asyncio
    async def test_is_called_with_creation_options_that_list_the_web_mercator_reprojection_as_the_source_uri(
        self,
        get_asset_dummy,
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
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = symbology_info.function.call_args_list[-1]
        (_, source_uri) = reprojection
        assert args[3].source_uri == [source_uri]

    @pytest.mark.asyncio
    async def test_is_called_with_zoom_level_and_max_zoom(
        self,
        get_asset_dummy,
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
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = symbology_info.function.call_args_list[-1]
        zoom_level, max_zoom = args[4:6]
        assert (zoom_level, max_zoom) == (0, 0)

    @pytest.mark.asyncio
    async def test_is_called_with_all_jobs_for_the_current_zoom_level(
        self,
        get_asset_dummy,
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
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = symbology_info.function.call_args_list[-1]
        reprojection_job = reprojection[0]
        assert args[6] == {0: {"source_reprojection_job": reprojection_job}}
