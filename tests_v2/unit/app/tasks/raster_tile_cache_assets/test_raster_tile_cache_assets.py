from datetime import datetime
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest

from app.models.orm.assets import Asset as ORMAsset
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.jobs import Job
from app.tasks.raster_tile_cache_assets import raster_tile_cache_asset
from app.tasks.raster_tile_cache_assets.symbology import SymbologyInfo


@patch("app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.execute")
@patch(
    "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.symbology_constructor"
)
@patch(
    "app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.reproject_to_web_mercator"
)
@patch("app.tasks.raster_tile_cache_assets.raster_tile_cache_assets.get_asset")
@pytest.mark.asyncio
async def test_it_does_something(
    get_asset_dummy, web_mercator_dummy, symbology_constructor_dummy, execute_mock
):
    get_asset_dummy.return_value = ORMAsset(
        creation_options={
            "pixel_meaning": "test pixels",
            "data_type": "boolean",
            "grid": "1/4000",
        }
    )
    web_mercator_dummy.return_value = (
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

    symbology_constructor_dummy.__getitem__.return_value = SymbologyInfo(
        8, 1, AsyncMock(return_value=([], "myjob"))
    )
    execute_mock.return_value = ChangeLog(
        date_time=datetime(2022, 12, 6), status="success", message="All done!"
    )

    TILE_CACHE_ASSET_UUID = UUID("e0a2dc44-aee1-4f71-963b-70a85869685d")
    SOURCE_ASSET_UUID = UUID("a8230040-23ad-4def-aca3-a292b6161557")

    CREATION_OPTIONS = {
        "creation_options": {
            "min_zoom": 0,
            "max_zoom": 0,
            "max_static_zoom": 0,
            "implementation": "default",
            "symbology": {
                "type": "date_conf_intensity",
            },
            "resampling": "nearest",
            "source_asset_id": SOURCE_ASSET_UUID,
        }
    }

    result = await raster_tile_cache_asset(
        "test_dataset", "2022", TILE_CACHE_ASSET_UUID, CREATION_OPTIONS
    )

    assert result.message == "All done!"
