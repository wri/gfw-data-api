from datetime import datetime
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest

from app.models.enum.change_log import ChangeLogStatus
from app.models.orm.assets import Asset as ORMAsset
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.jobs import Job
from app.tasks.raster_tile_cache_assets import raster_tile_cache_asset
from app.tasks.raster_tile_cache_assets.symbology import SymbologyInfo


@pytest.fixture()
def source_asset():
    return ORMAsset(
        creation_options={
            "pixel_meaning": "test pixels",
            "data_type": "boolean",
            "grid": "1/4000",
        }
    )


@pytest.fixture()
def reprojection():
    return (
        Job(
            dataset="test_dataset",
            job_name="job",
            job_queue="job queue",
            job_definition="job definition",
            command=["doit"],
            vcpus=1,
            memory=64,
            attempts=1,
            attempt_duration_seconds=1,
        ),
        "https://some/asset/uri",
    )


@pytest.fixture()
def symbology_info():
    return SymbologyInfo(8, 1, AsyncMock(return_value=([], "myjob")))


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

    TILE_CACHE_ASSET_UUID = UUID("e0a2dc44-aee1-4f71-963b-70a85869685d")
    SOURCE_ASSET_UUID = UUID("a8230040-23ad-4def-aca3-a292b6161557")

    CREATION_OPTIONS = {
        "creation_options": {
            "min_zoom": 0,
            "max_zoom": 0,  # make sure to test this at least with 1
            "max_static_zoom": 0,
            "implementation": "default",
            "symbology": {
                "type": "date_conf_intensity",  # try no_symbology
            },
            "resampling": "nearest",
            "source_asset_id": SOURCE_ASSET_UUID,
        }
    }

    result = await raster_tile_cache_asset(
        "test_dataset", "2022", TILE_CACHE_ASSET_UUID, CREATION_OPTIONS
    )

    assert result.status == ChangeLogStatus.success


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
async def test_source_asset_is_retrieved_by_uuid(
    get_asset_mock,
    web_mercator_dummy,
    symbology_constructor_dummy,
    execute_dummy,
    source_asset,
    reprojection,
    symbology_info,
    change_log,
):
    """Goal of this test is to determine the minimum amount of patching we need
    to do to get the function to run as much side-effect free code as
    possible."""
    get_asset_mock.return_value = source_asset
    symbology_constructor_dummy.__getitem__.return_value = symbology_info
    web_mercator_dummy.return_value = reprojection
    execute_dummy.return_value = change_log

    TILE_CACHE_ASSET_UUID = UUID("e0a2dc44-aee1-4f71-963b-70a85869685d")
    SOURCE_ASSET_UUID = UUID("a8230040-23ad-4def-aca3-a292b6161557")
    CREATION_OPTIONS = {
        "creation_options": {
            "min_zoom": 0,
            "max_zoom": 0,  # make sure to test this at least with 1
            "max_static_zoom": 0,
            "implementation": "default",
            "symbology": {
                "type": "date_conf_intensity",  # try no_symbology
            },
            "resampling": "nearest",
            "source_asset_id": SOURCE_ASSET_UUID,
        }
    }

    await raster_tile_cache_asset(
        "test_dataset", "2022", TILE_CACHE_ASSET_UUID, CREATION_OPTIONS
    )

    get_asset_mock.assert_called_with(SOURCE_ASSET_UUID)


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
async def test_reproject_to_web_mercator_is_called_with_dataset_and_version(
    get_asset_dummy,
    web_mercator_mock,
    symbology_constructor_dummy,
    execute_dummy,
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
    web_mercator_mock.return_value = reprojection
    execute_dummy.return_value = change_log

    TILE_CACHE_ASSET_UUID = UUID("e0a2dc44-aee1-4f71-963b-70a85869685d")
    SOURCE_ASSET_UUID = UUID("a8230040-23ad-4def-aca3-a292b6161557")
    CREATION_OPTIONS = {
        "creation_options": {
            "min_zoom": 0,
            "max_zoom": 0,  # make sure to test this at least with 1
            "max_static_zoom": 0,
            "implementation": "default",
            "symbology": {
                "type": "date_conf_intensity",  # try no_symbology
            },
            "resampling": "nearest",
            "source_asset_id": SOURCE_ASSET_UUID,
        }
    }

    await raster_tile_cache_asset(
        "test_dataset", "2022", TILE_CACHE_ASSET_UUID, CREATION_OPTIONS
    )

    args, _ = web_mercator_mock.call_args_list[-1]
    assert args[:2] == ("test_dataset", "2022"), "`dataset` and `version` do not match"


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
async def test_reproject_to_web_mercator_is_called_same_max_zoom_level_as_passed_in_creation_options(
    get_asset_dummy,
    web_mercator_mock,
    symbology_constructor_dummy,
    execute_dummy,
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
    web_mercator_mock.return_value = reprojection
    execute_dummy.return_value = change_log

    TILE_CACHE_ASSET_UUID = UUID("e0a2dc44-aee1-4f71-963b-70a85869685d")
    SOURCE_ASSET_UUID = UUID("a8230040-23ad-4def-aca3-a292b6161557")
    CREATION_OPTIONS = {
        "creation_options": {
            "min_zoom": 0,
            "max_zoom": 0,  # make sure to test this at least with 1
            "max_static_zoom": 0,
            "implementation": "default",
            "symbology": {
                "type": "date_conf_intensity",  # try no_symbology
            },
            "resampling": "nearest",
            "source_asset_id": SOURCE_ASSET_UUID,
        }
    }

    await raster_tile_cache_asset(
        "test_dataset", "2022", TILE_CACHE_ASSET_UUID, CREATION_OPTIONS
    )

    args, _ = web_mercator_mock.call_args_list[-1]
    assert args[3:5] == (
        0,
        0,
    ), "`zoom_level` and `max_zoom` values were not as expected"


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
async def test_reproject_to_web_mercator_is_called_with_resampling_kwargs(
    get_asset_dummy,
    web_mercator_mock,
    symbology_constructor_dummy,
    execute_dummy,
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
    web_mercator_mock.return_value = reprojection
    execute_dummy.return_value = change_log

    TILE_CACHE_ASSET_UUID = UUID("e0a2dc44-aee1-4f71-963b-70a85869685d")
    SOURCE_ASSET_UUID = UUID("a8230040-23ad-4def-aca3-a292b6161557")
    CREATION_OPTIONS = {
        "creation_options": {
            "min_zoom": 0,
            "max_zoom": 0,  # make sure to test this at least with 1
            "max_static_zoom": 0,
            "implementation": "default",
            "symbology": {
                "type": "date_conf_intensity",  # try no_symbology
            },
            "resampling": "nearest",
            "source_asset_id": SOURCE_ASSET_UUID,
        }
    }

    await raster_tile_cache_asset(
        "test_dataset", "2022", TILE_CACHE_ASSET_UUID, CREATION_OPTIONS
    )

    _, kwargs = web_mercator_mock.call_args_list[-1]
    assert kwargs == {
        "max_zoom_resampling": "nearest",
        "max_zoom_calc": None,
        "use_resampler": True,
    }, "`Resampling` arguments do not match"
