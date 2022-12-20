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
class TestCreateTileCacheCollaboration:
    @pytest.mark.asyncio
    async def test_is_called_dataset_and_version(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
        callback_constructor_dummy,
        create_tile_cache_mock,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        job,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        create_tile_cache_mock.return_value = job
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = create_tile_cache_mock.call_args_list[-1]
        assert args[:2] == ("test_dataset", "2022")

    @pytest.mark.asyncio
    async def test_is_called_with_symbology_asset_uri(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
        callback_constructor_dummy,
        create_tile_cache_mock,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        job,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        create_tile_cache_mock.return_value = job
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = create_tile_cache_mock.call_args_list[-1]
        assert args[2] == "/dummy/symbology/uri"

    @pytest.mark.asyncio
    async def test_is_called_with_zoom_level(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
        callback_constructor_dummy,
        create_tile_cache_mock,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        job,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        create_tile_cache_mock.return_value = job
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = create_tile_cache_mock.call_args_list[-1]
        assert args[3] == 0

    @pytest.mark.asyncio
    async def test_is_called_with_implementation_from_input_data_creation_options(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
        callback_constructor_dummy,
        create_tile_cache_mock,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        job,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        create_tile_cache_mock.return_value = job
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = create_tile_cache_mock.call_args_list[-1]
        assert args[4] == "default"

    @pytest.mark.asyncio
    async def test_sets_the_context_for_the_task_factory_as_the_tile_cache_asset_uuid(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
        callback_constructor_mock,
        create_tile_cache_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        job,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        create_tile_cache_dummy.return_value = job
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        callback_constructor_mock.assert_called_with(tile_cache_asset_uuid)

    @pytest.mark.asyncio
    async def test_is_called_with_the_task_factory(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
        callback_constructor_dummy,
        create_tile_cache_mock,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        job,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        create_tile_cache_mock.return_value = job
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = create_tile_cache_mock.call_args_list[-1]
        assert (
            args[5] == callback_constructor_dummy.return_value
        ), "Task factory wasn't passed to `create_tile_cache`"

    @pytest.mark.asyncio
    async def test_is_called_with_symbology_job(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
        callback_constructor_dummy,
        create_tile_cache_mock,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        job,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        create_tile_cache_mock.return_value = job
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = create_tile_cache_mock.call_args_list[-1]
        assert args[6][0].job_definition == "symbology job"

    @pytest.mark.asyncio
    async def test_is_called_with_a_source_reprojection_job(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
        callback_constructor_dummy,
        create_tile_cache_mock,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        job,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        create_tile_cache_mock.return_value = job
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = create_tile_cache_mock.call_args_list[-1]
        assert args[6][-1].job_definition == "source reprojection job"

    @pytest.mark.asyncio
    async def test_is_called_with_a_bit_depth_from_the_symbology_info(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
        callback_constructor_dummy,
        create_tile_cache_mock,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        symbology_info,
        job,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        create_tile_cache_mock.return_value = job
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = create_tile_cache_mock.call_args_list[-1]
        assert args[7] == 8
