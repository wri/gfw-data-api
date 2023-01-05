from datetime import datetime
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from app.models.orm.assets import Asset as ORMAsset
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import RasterTileSetSourceCreationOptions
from app.models.pydantic.jobs import Job
from app.models.pydantic.symbology import Symbology
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
def max_zoom_and_min_zoom_different_creation_options_dict(source_asset_uuid):
    return {
        "creation_options": {
            "min_zoom": 0,
            "max_zoom": 1,
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
