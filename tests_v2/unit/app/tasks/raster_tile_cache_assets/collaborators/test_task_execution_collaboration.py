from unittest.mock import patch

import pytest

from app.tasks.raster_tile_cache_assets import raster_tile_cache_asset

from . import MODULE_PATH_UNDER_TEST


@patch(f"{MODULE_PATH_UNDER_TEST}.execute", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.create_tile_cache", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.callback_constructor", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.symbology_constructor", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.reproject_to_web_mercator", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.get_asset", autospec=True)
class TestTaskExecutionCollaboration:
    @pytest.mark.asyncio
    async def test_is_called_with_complete_job_list_for_execution(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
        callback_constructor_dummy,
        create_tile_cache_dummy,
        execute_mock,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        tile_cache_job,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        create_tile_cache_dummy.return_value = tile_cache_job
        execute_mock.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = execute_mock.call_args_list[-1]
        result = [job.job_name for job in args[0]]
        assert result == ["reprojection_job", "symbology_job", "tile_cache_job"]

    @pytest.mark.asyncio
    async def test_is_called_with_complete_job_list_for_execution_with_multiple_zoom_levels(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
        callback_constructor_dummy,
        create_tile_cache_dummy,
        execute_mock,
        tile_cache_asset_uuid,
        max_zoom_and_min_zoom_different_creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        tile_cache_job,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        create_tile_cache_dummy.return_value = tile_cache_job
        execute_mock.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset",
            "2022",
            tile_cache_asset_uuid,
            max_zoom_and_min_zoom_different_creation_options_dict,
        )

        args, _ = execute_mock.call_args_list[-1]
        result = [job.job_name for job in args[0]]
        assert result == [
            "reprojection_job",  # zoom level 1
            "symbology_job",  # zoom level 1
            "reprojection_job",  # zoom level 0
            "symbology_job",  # zoom level 0
            "tile_cache_job",  # zoom level 0 - b/c zoom level is <= max static zoom
        ]
