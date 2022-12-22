from unittest.mock import patch

import pytest

from app.models.enum.change_log import ChangeLogStatus
from app.tasks.raster_tile_cache_assets import raster_tile_cache_asset

from . import MODULE_PATH_UNDER_TEST


@patch(f"{MODULE_PATH_UNDER_TEST}.execute", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.symbology_constructor", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.reproject_to_web_mercator", autospec=True)
@patch(f"{MODULE_PATH_UNDER_TEST}.get_asset", autospec=True)
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
