from unittest.mock import patch

import pytest

from app.models.orm.assets import Asset as ORMAsset
from app.models.pydantic.statistics import BandStats
from app.tasks.raster_tile_cache_assets import raster_tile_cache_asset
from app.tasks.raster_tile_cache_assets.symbology import SymbologyInfo, no_symbology

from . import MODULE_PATH_UNDER_TEST


@pytest.fixture()
def no_symbology_info():
    return SymbologyInfo(
        8,
        1,
        no_symbology,
    )


@pytest.fixture()
def float_source_asset():
    return ORMAsset(
        creation_options={
            "pixel_meaning": "test_pixels",
            "data_type": "float16",
            "grid": "1/4000",
        },
        stats={
            "bands": [
                BandStats(
                    min=0,
                    max=1,
                    mean=0.5,
                )
            ]
        },
    )


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
    async def test_is_called_with_asset_creation_options_pixel_meaning_modified(
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
    async def test_is_called_with_asset_creation_options_pixel_meaning_not_modified_due_to_no_symbology(
        self,
        get_asset_dummy,
        web_mercator_mock,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        reprojection,
        no_symbology_info,
        change_log,
        raster_tile_set_source_creation_options,
    ):
        get_asset_dummy.return_value = source_asset
        symbology_constructor_dummy.__getitem__.return_value = no_symbology_info
        web_mercator_mock.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        args, _ = web_mercator_mock.call_args_list[-1]
        assert args[2].pixel_meaning == "default"

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
    async def test_is_called_with_source_reprojection_parent_jobs(
        self,
        get_asset_dummy,
        web_mercator_mock,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        max_zoom_and_min_zoom_different_creation_options_dict,
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
            "test_dataset",
            "2022",
            tile_cache_asset_uuid,
            max_zoom_and_min_zoom_different_creation_options_dict,
        )

        args, _ = web_mercator_mock.call_args_list[-1]
        assert args[5] == [reprojection[0]]

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

    @pytest.mark.asyncio
    async def test_is_called_with_resampling_kwargs_when_data_type_is_converted_from_float_to_int(
        self,
        get_asset_dummy,
        web_mercator_mock,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        float_source_asset,
        reprojection,
        symbology_info,
        change_log,
    ):
        """Tests the scenario when the raster source creation options data type
        is a float."""
        get_asset_dummy.return_value = float_source_asset
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_mock.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        _, kwargs = web_mercator_mock.call_args_list[-1]
        assert kwargs == {
            "max_zoom_resampling": "nearest",
            "max_zoom_calc": "(A != None).astype(bool) * (1 + (A - 0.0) * 65534.0).astype(np.uint16)",
            "use_resampler": False,
        }, "`Resampling` arguments do not match"
