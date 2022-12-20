from unittest.mock import patch

import pytest

from app.tasks.raster_tile_cache_assets import raster_tile_cache_asset


@patch(
    "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.execute",
    autospec=True,
)
@patch(
    "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.create_tile_cache",
    autospec=True,
)
@patch(
    "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.callback_constructor",
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
class TestTaskExecutionIntegration:
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
