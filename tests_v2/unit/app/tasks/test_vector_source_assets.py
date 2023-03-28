from datetime import datetime
from typing import Dict, List, Optional, Set
from unittest.mock import Mock, patch
from uuid import UUID

import pytest as pytest

from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import FieldType
from app.models.pydantic.jobs import GdalPythonImportJob, Job, PostgresqlClientJob
from app.tasks.vector_source_assets import (
    _create_add_gfw_fields_job,
    _create_load_csv_data_jobs,
    _create_load_other_data_jobs,
    _create_vector_schema_job,
    append_vector_source_asset,
    vector_source_asset,
)

MODULE_PATH_UNDER_TEST = "app.tasks.vector_source_assets"

TEST_JOB_ENV: List[Dict[str, str]] = [{"name": "CORES", "value": "1"}]

DATASET: str = "some_dataset"
VERSION: str = "v42"
SOURCE_URI: str = "s3://bucket/test.shp"
LAYER: str = "test"
ZIPPED: bool = False
TABLE_SCHEMA: Optional[List[FieldType]] = None
CREATION_OPTIONS = {
    "source_type": "vector",
    "source_uri": ["s3://some_bucket/some_source_uri.zip"],
    "source_driver": "ESRI Shapefile",
    "cluster": None,
    "add_to_geostore": False,
    "table_schema": None,
    "layers": None,
    "indices": [],
}
VECTOR_ASSET_UUID = UUID("1b368160-caf8-2bd7-819a-ad4949361f02")


async def dummy_function():
    pass


async def mock_callback(task_id: UUID, change_log: ChangeLog):
    return dummy_function


class TestVectorSourceAssetsHelpers:
    @pytest.mark.asyncio
    async def test__create_vector_schema_job_no_schema(self):

        job = await _create_vector_schema_job(
            DATASET,
            VERSION,
            SOURCE_URI,
            LAYER,
            ZIPPED,
            TABLE_SCHEMA,
            TEST_JOB_ENV,
            mock_callback,
        )

        assert isinstance(job, GdalPythonImportJob)
        assert job.parents is None
        assert "-m" not in job.command

    @pytest.mark.asyncio
    async def test__create_vector_schema_job_with_schema(self):
        different_table_schema: Optional[List[FieldType]] = [
            FieldType(**{"name": "fid", "data_type": "numeric"}),
            FieldType(**{"name": "geom", "data_type": "geometry"}),
        ]

        job = await _create_vector_schema_job(
            DATASET,
            VERSION,
            SOURCE_URI,
            LAYER,
            ZIPPED,
            different_table_schema,
            TEST_JOB_ENV,
            mock_callback,
        )

        assert isinstance(job, GdalPythonImportJob)
        schema_arg_observed = False
        for i, cmd_frag in enumerate(job.command):
            if cmd_frag == "-m":
                schema_arg_observed = True
                assert job.command[i + 1] == (
                    '[{"name": "fid", "data_type": "numeric"}, '
                    '{"name": "geom", "data_type": "geometry"}]'
                )
                break
        assert schema_arg_observed, "Table schema arg not observed"

    @pytest.mark.asyncio
    async def test__create_vector_schema_job_zipped(self):
        zipped_true: bool = True

        job = await _create_vector_schema_job(
            DATASET,
            VERSION,
            SOURCE_URI,
            LAYER,
            zipped_true,
            TABLE_SCHEMA,
            TEST_JOB_ENV,
            mock_callback,
        )

        assert isinstance(job, GdalPythonImportJob)
        zip_arg_observed = False
        for i, cmd_frag in enumerate(job.command):
            if cmd_frag == "-X":
                zip_arg_observed = True
                assert job.command[i + 1] == "True"
                break
        assert zip_arg_observed, "Zip argument not found when it should be"

    @pytest.mark.asyncio
    async def test__create_vector_schema_job_not_zipped(self):

        job = await _create_vector_schema_job(
            DATASET,
            VERSION,
            SOURCE_URI,
            LAYER,
            ZIPPED,
            TABLE_SCHEMA,
            TEST_JOB_ENV,
            mock_callback,
        )

        assert isinstance(job, GdalPythonImportJob)
        zip_arg_observed = False
        for i, cmd_frag in enumerate(job.command):
            if cmd_frag == "-X":
                zip_arg_observed = True
                assert job.command[i + 1] == "False"
                break
        assert zip_arg_observed, "Zip argument found when it shouldn't be"

    @pytest.mark.asyncio
    async def test__create_add_gfw_fields_job(self):
        parents: List[str] = ["some_job"]
        attempt_duration_seconds: int = 100

        job = await _create_add_gfw_fields_job(
            DATASET,
            VERSION,
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

        source_uris: List[str] = [
            "s3://bucket/some_key.shp",
            "gs://bucket/some_other_key.shp",
            "gs://bucket/yet_another_key.shp",
        ]
        parents: List[str] = ["some_job", "some_other_job"]
        attempt_duration_seconds: int = 100

        jobs = await _create_load_csv_data_jobs(
            DATASET,
            VERSION,
            source_uris,
            TABLE_SCHEMA,
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

        source_uris: List[str] = [
            "s3://bucket/some_key.shp",
            "gs://bucket/some_other_key.shp",
            "gs://bucket/yet_another_key.shp",
        ]
        parents: List[str] = ["add_gfw_fields"]
        attempt_duration_seconds: int = 100

        jobs = await _create_load_csv_data_jobs(
            DATASET,
            VERSION,
            source_uris,
            TABLE_SCHEMA,
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

        source_args_observed = 0
        source_uris_set = set(source_uris)
        for i, cmd_frag in enumerate(job.command):
            if cmd_frag == "-s":
                source_args_observed += 1
                assert job.command[i + 1] in source_uris_set
        assert source_args_observed == 3

    @pytest.mark.asyncio
    @patch(f"{MODULE_PATH_UNDER_TEST}.min")
    async def test__create_load_other_data_jobs_1_queue(self, mock_min):
        mock_min.return_value = 1

        layers = ["layer1", "layer2", "layer3"]
        parents: List[str] = ["some_job"]
        attempt_duration_seconds: int = 100

        jobs, _ = await _create_load_other_data_jobs(
            DATASET,
            VERSION,
            SOURCE_URI,
            layers,
            ZIPPED,
            TABLE_SCHEMA,
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

        layers = ["layer1", "layer2", "layer3"]
        parents: List[str] = ["some_job"]
        attempt_duration_seconds: int = 100

        jobs, _ = await _create_load_other_data_jobs(
            DATASET,
            VERSION,
            SOURCE_URI,
            layers,
            ZIPPED,
            TABLE_SCHEMA,
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


@patch(f"{MODULE_PATH_UNDER_TEST}.execute", autospec=True)
class TestVectorSourceAssets:
    @pytest.mark.asyncio
    async def test_vector_source_asset_minimal(
        self,
        mock_execute: Mock,
    ):
        mock_execute.return_value = ChangeLog(
            date_time=datetime(2022, 12, 20), message="All done!", status="success"
        )

        _ = await vector_source_asset(
            DATASET,
            VERSION,
            VECTOR_ASSET_UUID,
            {"creation_options": CREATION_OPTIONS},
        )

        assert mock_execute.call_count == 1
        jobs: List[Job] = mock_execute.call_args_list[0].args[0]
        assert len(jobs) == 4

        expected_load_csv_data_jobs: int = 0
        observed_load_csv_data_jobs: int = 0
        expected_load_other_data_jobs: int = 1
        observed_load_other_data_jobs: int = 0

        for job in jobs:
            assert job.job_name != "cluster_table"
            assert job.job_name != "inherit_from_geostore"
            assert not job.job_name.startswith("create_index_")
            if job.job_name.startswith("load_vector_csv_data_"):
                observed_load_csv_data_jobs += 1
            elif job.job_name.startswith("load_vector_data_layer_"):
                observed_load_other_data_jobs += 1

        assert observed_load_csv_data_jobs == expected_load_csv_data_jobs
        assert observed_load_other_data_jobs == expected_load_other_data_jobs

    @pytest.mark.asyncio
    async def test_vector_source_asset_minimal_csv(
        self,
        mock_execute: Mock,
    ):
        mock_execute.return_value = ChangeLog(
            date_time=datetime(2022, 12, 20), message="All done!", status="success"
        )

        _ = await vector_source_asset(
            DATASET,
            VERSION,
            VECTOR_ASSET_UUID,
            {"creation_options": CREATION_OPTIONS | {"source_driver": "CSV"}},
        )

        assert mock_execute.call_count == 1
        jobs: List[Job] = mock_execute.call_args_list[0].args[0]
        assert len(jobs) == 4

        expected_load_csv_data_jobs: int = 1
        observed_load_csv_data_jobs: int = 0
        expected_load_other_data_jobs: int = 0
        observed_load_other_data_jobs: int = 0

        for job in jobs:
            assert job.job_name != "cluster_table"
            assert job.job_name != "inherit_from_geostore"
            assert not job.job_name.startswith("create_index_")
            if job.job_name.startswith("load_vector_csv_data_"):
                observed_load_csv_data_jobs += 1
            elif job.job_name.startswith("load_vector_data_layer_"):
                observed_load_other_data_jobs += 1

        assert observed_load_csv_data_jobs == expected_load_csv_data_jobs
        assert observed_load_other_data_jobs == expected_load_other_data_jobs

    @pytest.mark.asyncio
    async def test_vector_source_asset_geostore_enabled(
        self,
        mock_execute: Mock,
    ):
        mock_execute.return_value = ChangeLog(
            date_time=datetime(2022, 12, 20), message="All done!", status="success"
        )

        _ = await vector_source_asset(
            DATASET,
            VERSION,
            VECTOR_ASSET_UUID,
            {"creation_options": CREATION_OPTIONS | {"add_to_geostore": True}},
        )

        assert mock_execute.call_count == 1
        jobs: List[Job] = mock_execute.call_args_list[0].args[0]
        assert len(jobs) == 5

        expected_geostore_jobs: int = 1
        observed_geostore_jobs: int = 0
        for job in jobs:
            if job.job_name == "inherit_from_geostore":
                observed_geostore_jobs += 1
        assert expected_geostore_jobs == observed_geostore_jobs

    @pytest.mark.asyncio
    async def test_vector_source_asset_geostore_default_indices(
        self,
        mock_execute: Mock,
    ):
        mock_execute.return_value = ChangeLog(
            date_time=datetime(2022, 12, 20), message="All done!", status="success"
        )

        input_data_c_o_copy = CREATION_OPTIONS.copy()
        input_data_c_o_copy.pop("indices")
        _ = await vector_source_asset(
            DATASET,
            VERSION,
            VECTOR_ASSET_UUID,
            {"creation_options": input_data_c_o_copy},
        )

        assert mock_execute.call_count == 1
        jobs: List[Job] = mock_execute.call_args_list[0].args[0]
        assert len(jobs) == 7

        expected_index_jobs: int = 3
        observed_index_jobs: int = 0
        for job in jobs:
            if job.job_name.startswith("create_index_"):
                observed_index_jobs += 1
        assert expected_index_jobs == observed_index_jobs

    @pytest.mark.asyncio
    async def test_vector_source_asset_geostore_with_clustering(
        self,
        mock_execute: Mock,
    ):
        mock_execute.return_value = ChangeLog(
            date_time=datetime(2022, 12, 20), message="All done!", status="success"
        )

        cluster_field = {
            "cluster": {
                "index_type": "btree",
                "column_names": ["gfw_geostore_id", "gfw_bbox"],
            }
        }

        _ = await vector_source_asset(
            DATASET,
            VERSION,
            VECTOR_ASSET_UUID,
            {"creation_options": CREATION_OPTIONS | cluster_field},
        )

        assert mock_execute.call_count == 1
        jobs: List[Job] = mock_execute.call_args_list[0].args[0]
        assert len(jobs) == 5

        expected_cluster_jobs: int = 1
        observed_cluster_jobs: int = 0
        for job in jobs:
            if job.job_name == "cluster_table":
                observed_cluster_jobs += 1
        assert expected_cluster_jobs == observed_cluster_jobs


@patch(f"{MODULE_PATH_UNDER_TEST}.execute", autospec=True)
class TestAppendVectorSourceAssets:
    @pytest.mark.asyncio
    async def test_append_vector_source_asset(
        self,
        mock_execute: Mock,
    ):
        mock_execute.return_value = ChangeLog(
            date_time=datetime(2022, 12, 20), message="All done!", status="success"
        )

        append_input_data_c_o = {
            "source_type": "vector",
            "source_uri": ["s3://some_bucket/some_source_uri.zip"],
            "source_driver": "ESRI Shapefile",
            "layers": None,
        }

        _ = await append_vector_source_asset(
            DATASET,
            VERSION,
            VECTOR_ASSET_UUID,
            {"creation_options": append_input_data_c_o},
        )

        assert mock_execute.call_count == 1
        jobs: List[Job] = mock_execute.call_args_list[0].args[0]
        assert len(jobs) == 2

        expected_load_csv_data_jobs: int = 0
        observed_load_csv_data_jobs: int = 0
        expected_load_other_data_jobs: int = 1
        observed_load_other_data_jobs: int = 0

        for job in jobs:
            assert job.job_name != "cluster_table"
            assert job.job_name != "inherit_from_geostore"
            assert not job.job_name.startswith("create_index_")
            if job.job_name.startswith("load_vector_csv_data_"):
                observed_load_csv_data_jobs += 1
            elif job.job_name.startswith("load_vector_data_layer_"):
                observed_load_other_data_jobs += 1

        assert observed_load_csv_data_jobs == expected_load_csv_data_jobs
        assert observed_load_other_data_jobs == expected_load_other_data_jobs

    @pytest.mark.asyncio
    async def test_append_vector_source_asset_csv(
        self,
        mock_execute: Mock,
    ):
        mock_execute.return_value = ChangeLog(
            date_time=datetime(2022, 12, 20), message="All done!", status="success"
        )

        append_input_data_c_o = {
            "source_type": "vector",
            "source_uri": ["s3://some_bucket/some_source_uri.zip"],
            "source_driver": "CSV",
            "layers": None,
        }

        _ = await append_vector_source_asset(
            DATASET,
            VERSION,
            VECTOR_ASSET_UUID,
            {"creation_options": append_input_data_c_o},
        )

        assert mock_execute.call_count == 1
        jobs: List[Job] = mock_execute.call_args_list[0].args[0]
        assert len(jobs) == 2

        expected_load_csv_data_jobs: int = 1
        observed_load_csv_data_jobs: int = 0
        expected_load_other_data_jobs: int = 0
        observed_load_other_data_jobs: int = 0

        for job in jobs:
            assert job.job_name != "cluster_table"
            assert job.job_name != "inherit_from_geostore"
            assert not job.job_name.startswith("create_index_")
            if job.job_name.startswith("load_vector_csv_data_"):
                observed_load_csv_data_jobs += 1
            elif job.job_name.startswith("load_vector_data_layer_"):
                observed_load_other_data_jobs += 1

        assert observed_load_csv_data_jobs == expected_load_csv_data_jobs
        assert observed_load_other_data_jobs == expected_load_other_data_jobs
