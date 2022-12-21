from datetime import datetime
from unittest.mock import Mock, patch
from uuid import UUID

import pytest as pytest

from app.models.pydantic.change_log import ChangeLog
from app.tasks.vector_source_assets import vector_source_asset

MODULE_PATH_UNDER_TEST = "app.tasks.vector_source_assets"

input_data = {
    "creation_options": {
        "source_type": "vector",
        "source_uri": ["s3://some_bucket/some_source_uri.zip"],
        "source_driver": "ESRI Shapefile",
    },
}


# @patch(f"{MODULE_PATH_UNDER_TEST}.execute", autospec=True)
# class TestVectorSourceAssets:
#
#     @pytest.mark.asyncio
#     async def test_vector_source_asset(
#         self,
#         mock_execute,
#         batch_client,
#         async_client: AsyncClient
#     ):
#         mock_execute.return_value = change_log
#
#         await vector_source_asset(
#             "test_dataset", "2022", tile_cache_asset_uuid, creation_options_dict
#         )
#
#         args, _ = create_tile_cache_mock.call_args_list[-1]
#         assert args[:2] == ("test_dataset", "2022")


@patch(f"{MODULE_PATH_UNDER_TEST}.execute", autospec=True)
# @patch(f"{MODULE_PATH_UNDER_TEST}._create_vector_schema_job", autospec=True)
class TestVectorSourceAssetsHelpers:
    @pytest.mark.asyncio
    async def test__create_vector_schema_job(
        self,
        # mock__create_vector_schema_job,
        mock_execute: Mock,
    ):
        mock_execute.return_value = ChangeLog(
            date_time=datetime(2022, 12, 20), message="All done!", status="success"
        )

        vector_asset_uuid = UUID("1b368160-caf8-2bd7-819a-ad4949361f02")

        # mock_default_asset.return_value = ORMAsset(
        #     creation_options={
        #         "pixel_meaning": "test_pixels",
        #         "data_type": "boolean",
        #         "grid": "1/4000",
        #     }
        # )

        _ = await vector_source_asset(
            "test_dataset", "v2022", vector_asset_uuid, input_data
        )

        assert mock_execute.call_count == 1
        jobs = mock_execute.call_args_list[0].args[0]
        assert len(jobs) == 8
