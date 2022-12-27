from datetime import datetime
from typing import Dict, List, Optional, Set
from unittest.mock import Mock, patch
from uuid import UUID

import pytest as pytest

from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import FieldType
from app.models.pydantic.jobs import GdalPythonImportJob, PostgresqlClientJob
from app.tasks.vector_source_assets import (
    _create_add_gfw_fields_job,
    _create_load_csv_data_jobs,
    _create_load_other_data_jobs,
    _create_vector_schema_job,
    vector_source_asset,
)

MODULE_PATH_UNDER_TEST = "app.tasks.vector_source_assets"

TEST_JOB_ENV: List[Dict[str, str]] = [{"name": "CORES", "value": "1"}]

input_data = {
    "creation_options": {
        "source_type": "vector",
        "source_uri": ["s3://some_bucket/some_source_uri.zip"],
        "source_driver": "ESRI Shapefile",
    },
}


async def dummy_function():
    pass


async def mock_callback(task_id: UUID, change_log: ChangeLog):
    return dummy_function


@patch(f"{MODULE_PATH_UNDER_TEST}.execute", autospec=True)
class TestVectorSourceAssets:
    @pytest.mark.asyncio
    async def test_vector_source_asset(
        self,
        mock_execute: Mock,
    ):
        mock_execute.return_value = ChangeLog(
            date_time=datetime(2022, 12, 20), message="All done!", status="success"
        )
        vector_asset_uuid = UUID("1b368160-caf8-2bd7-819a-ad4949361f02")

        _ = await vector_source_asset(
            "test_dataset", "v2022", vector_asset_uuid, input_data
        )

        assert mock_execute.call_count == 1
        jobs = mock_execute.call_args_list[0].args[0]
        assert len(jobs) == 8


class TestVectorSourceAssetsHelpers:
    @pytest.mark.asyncio
    async def test__create_vector_schema_job_no_schema(self):
        dataset: str = "some_dataset"
        version: str = "v42"
        source_uri: str = "s3://bucket/test.shp"
        layer: str = "test"
        zipped: bool = False
        table_schema: Optional[List[FieldType]] = None

        job = await _create_vector_schema_job(
            dataset,
            version,
            source_uri,
            layer,
            zipped,
            table_schema,
            TEST_JOB_ENV,
            mock_callback,
        )

        assert isinstance(job, GdalPythonImportJob)
        assert job.parents is None
        assert "-m" not in job.command

    @pytest.mark.asyncio
    async def test__create_vector_schema_job_with_schema(self):
        dataset: str = "some_dataset"
        version: str = "v42"
        source_uri: str = "s3://bucket/test.zip"
        layer: str = "test"
        zipped: bool = True
        table_schema: Optional[List[FieldType]] = [
            FieldType(**{"field_name": "fid", "field_type": "numeric"}),
            FieldType(**{"field_name": "geom", "field_type": "geometry"}),
        ]

        job = await _create_vector_schema_job(
            dataset,
            version,
            source_uri,
            layer,
            zipped,
            table_schema,
            TEST_JOB_ENV,
            mock_callback,
        )

        assert isinstance(job, GdalPythonImportJob)
        schema_arg_found = False
        for i, cmd_frag in enumerate(job.command):
            if cmd_frag == "-m":
                schema_arg_found = True
                assert job.command[i + 1] == (
                    '[{"field_name": "fid", "field_type": "numeric"}, '
                    '{"field_name": "geom", "field_type": "geometry"}]'
                )
                break
        assert schema_arg_found, "Table schema arg not found"

        # mock_default_asset.return_value = ORMAsset(
        #     creation_options={
        #         "pixel_meaning": "test_pixels",
        #         "data_type": "boolean",
        #         "grid": "1/4000",
        #     }
        # )

    @pytest.mark.asyncio
    async def test__create_vector_schema_job_zipped(self):
        dataset: str = "some_dataset"
        version: str = "v42"
        source_uri: str = "s3://bucket/test.shp"
        layer: str = "test"
        zipped: bool = True
        table_schema: Optional[List[FieldType]] = None

        job = await _create_vector_schema_job(
            dataset,
            version,
            source_uri,
            layer,
            zipped,
            table_schema,
            TEST_JOB_ENV,
            mock_callback,
        )

        assert isinstance(job, GdalPythonImportJob)
        zip_arg_found = False
        for i, cmd_frag in enumerate(job.command):
            if cmd_frag == "-X":
                zip_arg_found = True
                assert job.command[i + 1] == "True"
                break
        assert zip_arg_found, "Zip argument not found"

    @pytest.mark.asyncio
    async def test__create_vector_schema_job_not_zipped(self):
        dataset: str = "some_dataset"
        version: str = "v42"
        source_uri: str = "s3://bucket/test.shp"
        layer: str = "test"
        zipped: bool = False
        table_schema: Optional[List[FieldType]] = None

        job = await _create_vector_schema_job(
            dataset,
            version,
            source_uri,
            layer,
            zipped,
            table_schema,
            TEST_JOB_ENV,
            mock_callback,
        )

        assert isinstance(job, GdalPythonImportJob)
        zip_arg_found = False
        for i, cmd_frag in enumerate(job.command):
            if cmd_frag == "-X":
                zip_arg_found = True
                assert job.command[i + 1] == "False"
                break
        assert zip_arg_found, "Zip argument not found"

    @pytest.mark.asyncio
    async def test__create_add_gfw_fields_job(self):
        dataset: str = "some_dataset"
        version: str = "v42"
        parents: List[str] = ["some_job"]
        attempt_duration_seconds: int = 100

        job = await _create_add_gfw_fields_job(
            dataset,
            version,
            parents,
            TEST_JOB_ENV,
            mock_callback,
            attempt_duration_seconds,
        )

        assert isinstance(job, PostgresqlClientJob)
        assert job.parents == ["some_job"]
        assert job.attempt_duration_seconds == attempt_duration_seconds

    @pytest.mark.asyncio
    @patch(f"{MODULE_PATH_UNDER_TEST}.chunk_list", autospec=True)
    async def test__create_load_csv_data_jobs_3_chunks(self, mock_chunk_list):
        mock_chunk_list.return_value = [
            ["s3://bucket/some_key.shp"],
            ["gs://bucket/some_other_key.shp"],
            ["gs://bucket/yet_another_key.shp"],
        ]

        dataset: str = "some_dataset"
        version: str = "v42"
        source_uris: List[str] = [
            "s3://bucket/some_key.shp",
            "gs://bucket/some_other_key.shp",
            "gs://bucket/yet_another_key.shp",
        ]
        parents: List[str] = ["some_job", "some_other_job"]
        attempt_duration_seconds: int = 100

        jobs = await _create_load_csv_data_jobs(
            dataset,
            version,
            source_uris,
            parents,
            TEST_JOB_ENV,
            mock_callback,
            attempt_duration_seconds,
        )

        assert len(jobs) == 3
        for job in jobs:
            assert isinstance(job, GdalPythonImportJob)
            assert job.parents == parents
            assert job.attempt_duration_seconds == attempt_duration_seconds

        source_uris_set = set(source_uris)
        for job in jobs:
            for i, cmd_frag in enumerate(job.command):
                if cmd_frag == "-s":
                    assert job.command[i + 1] in source_uris_set
                    source_uris_set.discard(job.command[i + 1])
        assert len(source_uris_set) == 0

    @pytest.mark.asyncio
    @patch(f"{MODULE_PATH_UNDER_TEST}.chunk_list", autospec=True)
    async def test__create_load_csv_data_jobs_1_chunk(self, mock_chunk_list):
        mock_chunk_list.return_value = [
            [
                "s3://bucket/some_key.shp",
                "gs://bucket/some_other_key.shp",
                "gs://bucket/yet_another_key.shp",
            ]
        ]

        dataset: str = "some_dataset"
        version: str = "v42"
        source_uris: List[str] = [
            "s3://bucket/some_key.shp",
            "gs://bucket/some_other_key.shp",
            "gs://bucket/yet_another_key.shp",
        ]
        parents: List[str] = ["add_gfw_fields"]
        attempt_duration_seconds: int = 100

        jobs = await _create_load_csv_data_jobs(
            dataset,
            version,
            source_uris,
            parents,
            TEST_JOB_ENV,
            mock_callback,
            attempt_duration_seconds,
        )

        assert len(jobs) == 1
        job = jobs[0]
        assert isinstance(job, GdalPythonImportJob)
        assert job.parents == parents
        assert job.attempt_duration_seconds == attempt_duration_seconds

        source_args_found = 0
        source_uris_set = set(source_uris)
        for i, cmd_frag in enumerate(job.command):
            if cmd_frag == "-s":
                source_args_found += 1
                assert job.command[i + 1] in source_uris_set
        assert source_args_found == 3

    @pytest.mark.asyncio
    @patch(f"{MODULE_PATH_UNDER_TEST}.min")
    async def test__create_load_other_data_jobs_1_queue(self, mock_min):
        mock_min.return_value = 1

        dataset: str = "some_dataset"
        version: str = "v42"
        source_uri: str = "s3://bucket/some_key.shp"
        layers = ["layer1", "layer2", "layer3"]
        zipped = False
        parents: List[str] = ["some_job"]
        attempt_duration_seconds: int = 100

        jobs, _ = await _create_load_other_data_jobs(
            dataset,
            version,
            source_uri,
            layers,
            zipped,
            parents,
            TEST_JOB_ENV,
            mock_callback,
            attempt_duration_seconds,
        )

        assert len(jobs) == len(layers)

        # With only 1 queue, the 3 created jobs will be in series. The first
        # job will have the original parents, and the two successive jobs
        # will have one of the created jobs as a parent.
        observed_job_parents: Set = set()
        expected_job_parents = {
            *parents,
            "load_vector_data_layer_0",
            "load_vector_data_layer_1",
        }
        for job in jobs:
            assert isinstance(job, GdalPythonImportJob)
            assert len(job.parents) == 1
            observed_job_parents.add(job.parents[0])
            assert job.attempt_duration_seconds == attempt_duration_seconds

        assert observed_job_parents == expected_job_parents

    @pytest.mark.asyncio
    @patch(f"{MODULE_PATH_UNDER_TEST}.min")
    async def test__create_load_other_data_jobs_3_queues(self, mock_min):
        mock_min.return_value = 3

        dataset: str = "some_dataset"
        version: str = "v42"
        source_uri: str = "s3://bucket/some_key.shp"
        layers = ["layer1", "layer2", "layer3"]
        zipped = False
        parents: List[str] = ["some_job"]
        attempt_duration_seconds: int = 100

        jobs, _ = await _create_load_other_data_jobs(
            dataset,
            version,
            source_uri,
            layers,
            zipped,
            parents,
            TEST_JOB_ENV,
            mock_callback,
            attempt_duration_seconds,
        )

        assert len(jobs) == len(layers)

        # There's 3 queues, enough for each layer to have its own, so
        # each job will have the original parents
        for job in jobs:
            assert isinstance(job, GdalPythonImportJob)
            assert job.parents == parents
            assert job.attempt_duration_seconds == attempt_duration_seconds
