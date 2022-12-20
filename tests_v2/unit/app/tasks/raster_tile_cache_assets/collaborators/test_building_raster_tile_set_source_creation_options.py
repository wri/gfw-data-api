from unittest.mock import patch

import pytest

from app.models.enum.creation_options import RasterDrivers
from app.models.enum.sources import RasterSourceType
from app.models.pydantic.symbology import Symbology
from app.tasks.raster_tile_cache_assets import raster_tile_cache_asset

from . import MODULE_PATH_UNDER_TEST


def assert_contains_subset(kwargs, expected_subset):
    """Simple assertion helper function for asserting fewer keys than what's in
    the entire dict."""
    __tracebackhide__ = True
    assert expected_subset == {k: v for k, v in kwargs.items() if k in expected_subset}


@patch(f"{MODULE_PATH_UNDER_TEST}.execute", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.symbology_constructor", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.reproject_to_web_mercator", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.RasterTileSetSourceCreationOptions", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.get_asset", autospec=True)
class TestBuildingRasterTileSetSourceCreationOptionsFromSourceAsset:
    @pytest.mark.asyncio
    async def test_overrides_source_information(
        self,
        get_asset_dummy,
        raster_tile_set_source_creation_options_mock,
        web_mercator_dummy,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        raster_tile_set_source_creation_options,
        reprojection,
        symbology_info,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        raster_tile_set_source_creation_options_mock.return_value = (
            raster_tile_set_source_creation_options
        )
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        _, kwargs = raster_tile_set_source_creation_options_mock.call_args_list[-1]
        expected = {
            "source_type": RasterSourceType.raster,
            "source_driver": RasterDrivers.geotiff,
            "source_uri": [
                "s3://gfw-data-lake-test/test_dataset/2022/raster/epsg-4326/1/4000/test_pixels/geotiff/tiles.geojson"
            ],
        }
        assert_contains_subset(kwargs, expected)

    @pytest.mark.asyncio
    async def test_sets_calc_to_none(
        self,
        get_asset_dummy,
        raster_tile_set_source_creation_options_mock,
        web_mercator_dummy,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        raster_tile_set_source_creation_options,
        reprojection,
        symbology_info,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        raster_tile_set_source_creation_options_mock.return_value = (
            raster_tile_set_source_creation_options
        )
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        _, kwargs = raster_tile_set_source_creation_options_mock.call_args_list[-1]
        expected = {"calc": None}
        assert_contains_subset(kwargs, expected)

    @pytest.mark.asyncio
    async def test_sets_resampling_method_from_input_params(
        self,
        get_asset_dummy,
        raster_tile_set_source_creation_options_mock,
        web_mercator_dummy,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        raster_tile_set_source_creation_options,
        reprojection,
        symbology_info,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        raster_tile_set_source_creation_options_mock.return_value = (
            raster_tile_set_source_creation_options
        )
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        _, kwargs = raster_tile_set_source_creation_options_mock.call_args_list[-1]
        expected = {
            "resampling": creation_options_dict["creation_options"]["resampling"]
        }
        assert_contains_subset(kwargs, expected)

    @pytest.mark.asyncio
    async def test_sets_compute_information_to_false(
        self,
        get_asset_dummy,
        raster_tile_set_source_creation_options_mock,
        web_mercator_dummy,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        raster_tile_set_source_creation_options,
        reprojection,
        symbology_info,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        raster_tile_set_source_creation_options_mock.return_value = (
            raster_tile_set_source_creation_options
        )
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        _, kwargs = raster_tile_set_source_creation_options_mock.call_args_list[-1]
        expected = {
            "compute_stats": False,
            "compute_histogram": False,
        }
        assert_contains_subset(kwargs, expected)

    @pytest.mark.asyncio
    async def test_sets_symbology_from_input_data(
        self,
        get_asset_dummy,
        raster_tile_set_source_creation_options_mock,
        web_mercator_dummy,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        raster_tile_set_source_creation_options,
        reprojection,
        symbology_info,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        raster_tile_set_source_creation_options_mock.return_value = (
            raster_tile_set_source_creation_options
        )
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        _, kwargs = raster_tile_set_source_creation_options_mock.call_args_list[-1]
        expected = {
            "symbology": Symbology(
                **creation_options_dict["creation_options"]["symbology"]
            ),
        }
        assert_contains_subset(kwargs, expected)

    @pytest.mark.asyncio
    async def test_sets_subset_to_none(
        self,
        get_asset_dummy,
        raster_tile_set_source_creation_options_mock,
        web_mercator_dummy,
        symbology_constructor_dummy,
        execute_dummy,
        tile_cache_asset_uuid,
        creation_options_dict,
        source_asset,
        raster_tile_set_source_creation_options,
        reprojection,
        symbology_info,
        change_log,
    ):
        get_asset_dummy.return_value = source_asset
        raster_tile_set_source_creation_options_mock.return_value = (
            raster_tile_set_source_creation_options
        )
        symbology_constructor_dummy.__getitem__.return_value = symbology_info
        web_mercator_dummy.return_value = reprojection
        execute_dummy.return_value = change_log

        await raster_tile_cache_asset(
            "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
        )

        _, kwargs = raster_tile_set_source_creation_options_mock.call_args_list[-1]
        expected = {
            "subset": None,
        }
        assert_contains_subset(kwargs, expected)
