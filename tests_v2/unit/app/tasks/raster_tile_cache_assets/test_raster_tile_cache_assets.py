from datetime import datetime
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest

from app.models.enum.change_log import ChangeLogStatus
from app.models.enum.creation_options import RasterDrivers
from app.models.enum.sources import RasterSourceType
from app.models.orm.assets import Asset as ORMAsset
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from app.models.pydantic.jobs import Job
from app.models.pydantic.symbology import Symbology
from app.tasks.raster_tile_cache_assets import raster_tile_cache_asset
from app.tasks.raster_tile_cache_assets.symbology import (
    SymbologyFuncType,
    SymbologyInfo,
)


@pytest.fixture(scope="module")
def tile_cache_asset_uuid():
    return UUID("e0a2dc44-aee1-4f71-963b-70a85869685d")


@pytest.fixture(scope="module")
def source_asset_uuid():
    return UUID("a8230040-23ad-4def-aca3-a292b6161557")


@pytest.fixture()
def creation_options_dict(source_asset_uuid):
    return {
        "creation_options": {
            "min_zoom": 0,
            "max_zoom": 0,  # make sure to test this at least with 1
            "max_static_zoom": 0,
            "implementation": "default",
            "symbology": {
                "type": "date_conf_intensity",  # try no_symbology
            },
            "resampling": "nearest",
            "source_asset_id": source_asset_uuid,
        }
    }


@pytest.fixture()
def source_asset():
    return ORMAsset(
        creation_options={
            "pixel_meaning": "test_pixels",
            "data_type": "boolean",
            "grid": "1/4000",
        }
    )


@pytest.fixture()
def raster_tile_set_source_creation_options():
    return RasterTileSetSourceCreationOptions(
        pixel_meaning="test_pixels",
        data_type="boolean",
        grid="1/4000",
        source_type="raster",
        source_driver="GeoTIFF",
        symbology=Symbology(type="date_conf_intensity"),
    )


@pytest.fixture()
def tile_cache_job():
    return Job(
        dataset="test_dataset",
        job_name="tile_cache_job",
        job_queue="tile_cache_job_queue",
        job_definition="tile cache job",
        command=["doit"],
        vcpus=1,
        memory=64,
        attempts=1,
        attempt_duration_seconds=1,
    )


@pytest.fixture()
def symbology_job():
    return Job(
        dataset="test_dataset",
        job_name="symbology_job",
        job_queue="symbology_job_queue",
        job_definition="symbology job",
        command=["doit"],
        vcpus=1,
        memory=64,
        attempts=1,
        attempt_duration_seconds=1,
    )


@pytest.fixture()
def job():
    return Job(
        dataset="test_dataset",
        job_name="reprojection_job",
        job_queue="reprojection_job_queue",
        job_definition="source reprojection job",
        command=["doit"],
        vcpus=1,
        memory=64,
        attempts=1,
        attempt_duration_seconds=1,
    )


@pytest.fixture()
def reprojection(job):
    return (
        job,
        "https://some/asset/uri",
    )


@pytest.fixture()
def symbology_info(symbology_job):
    return SymbologyInfo(
        8,
        1,
        AsyncMock(
            spec_set=SymbologyFuncType,
            return_value=([symbology_job], "/dummy/symbology/uri"),
        ),
    )


@pytest.fixture()
def change_log():
    return ChangeLog(
        date_time=datetime(2022, 12, 6), status="success", message="All done!"
    )


@patch(
    "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.execute", autospec=True
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
@pytest.mark.asyncio
async def test_exploratory_test_runs_without_error(
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
    """Goal of this test is to determine the minimum amount of patching we need
    to do to get the function to run as much side-effect free code as
    possible."""
    get_asset_dummy.return_value = source_asset
    symbology_constructor_dummy.__getitem__.return_value = symbology_info
    web_mercator_dummy.return_value = reprojection
    execute_dummy.return_value = change_log

    result = await raster_tile_cache_asset(
        "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
    )

    assert result.status == ChangeLogStatus.success


class TestCRUDIntegration:
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


class TestBuildingRasterTileSetSourceCreationOptionsFromSourceAsset:
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
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.RasterTileSetSourceCreationOptions",
        autospec=True,
    )
    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.get_asset",
        autospec=True,
    )
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
        assert expected == {k: v for k, v in kwargs.items() if k in expected}

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
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.RasterTileSetSourceCreationOptions",
        autospec=True,
    )
    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.get_asset",
        autospec=True,
    )
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
        expected = {
            "calc": None,
        }
        assert expected == {k: v for k, v in kwargs.items() if k in expected}

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
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.RasterTileSetSourceCreationOptions",
        autospec=True,
    )
    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.get_asset",
        autospec=True,
    )
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
        assert expected == {k: v for k, v in kwargs.items() if k in expected}

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
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.RasterTileSetSourceCreationOptions",
        autospec=True,
    )
    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.get_asset",
        autospec=True,
    )
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
        assert expected == {k: v for k, v in kwargs.items() if k in expected}

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
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.RasterTileSetSourceCreationOptions",
        autospec=True,
    )
    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.get_asset",
        autospec=True,
    )
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
        assert expected == {k: v for k, v in kwargs.items() if k in expected}

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
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.RasterTileSetSourceCreationOptions",
        autospec=True,
    )
    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.get_asset",
        autospec=True,
    )
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
        assert expected == {k: v for k, v in kwargs.items() if k in expected}


class TestWebMercatorReProjectionIntegration:
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


class TestSymbologyFunctionIntegration:
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


class TestCreateTileCacheIntegration:
    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.execute",
        autospec=True,
    )
    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.create_tile_cache",
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
    @pytest.mark.asyncio
    async def test_is_called_dataset_and_version(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
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

    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.execute",
        autospec=True,
    )
    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.create_tile_cache",
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
    @pytest.mark.asyncio
    async def test_is_called_with_symbology_asset_uri(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
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

    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.execute",
        autospec=True,
    )
    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.create_tile_cache",
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
    @pytest.mark.asyncio
    async def test_is_called_with_zoom_level(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
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

    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.execute",
        autospec=True,
    )
    @patch(
        "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.create_tile_cache",
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
    @pytest.mark.asyncio
    async def test_is_called_with_implementation_from_input_data_creation_options(
        self,
        get_asset_dummy,
        web_mercator_dummy,
        symbology_constructor_dummy,
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


class TestTaskExecutionIntegration:
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
