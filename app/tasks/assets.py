from datetime import datetime
from typing import List, Dict, Any, Optional, Callable, Awaitable
from typing.io import IO

from .batch import execute
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.job import Job, PostgresqlClientJob, GdalPythonImportJob
from ..models.pydantic.source import SourceType
from ..settings.globals import AURORA_JOB_QUEUE
from ..utils.aws import split_s3_path, get_s3_client


async def create_default_asset(
    input_data: Dict[str, Any],
    file_obj: Optional[IO],
    callback: Callable[[Dict[str, Any]], Awaitable[None]],
) -> None:
    source_type = input_data["source_type"]
    source_uri = input_data["source_uri"]
    config_options = input_data["config_options"]
    # create default asset for version (in database)

    if file_obj:
        log: ChangeLog = await _inject_file(file_obj, source_uri[0])
        # TODO: log changes to version
        if log.status == "failed":
            # TODO: set version status to failed, aboard pipeline, roll back changes
            pass

            # Schedule batch job queues depending on source type
    if source_type == SourceType.vector:
        _vector_source_asset(source_uri, config_options, callback)
    elif source_type == SourceType.table:
        _table_source_asset(source_uri, config_options, callback)
    elif source_type == SourceType.raster:
        _raster_source_asset(source_uri, config_options, callback)
    else:
        raise ValueError(f"Unsupported asset source type {source_type})")


def _vector_source_asset(source_uri: List[str], config_options, callback):
    vector_import_job = GdalPythonImportJob(
        job_name="import vector data", command=["vector-import"], environment={},
    )

    repair_geometry_job = Job(
        job_name="repair geometry",
        command=["repair_geometry"],
        environment={},
        parents=[vector_import_job.job_name],
    )

    gfw_attribute_job = PostgresqlClientJob(
        job_name="enrich gfw attributes",
        command=["enrich_gfw_attributes"],
        environment={},
        parents=[repair_geometry_job.job_name],
    )

    index_jobs: List[Job] = list()

    for index in config_options.indices:
        index_jobs.append(
            PostgresqlClientJob(
                job_name="geom index",
                command=["build_index"],
                environment={},
                parents=[gfw_attribute_job.job_name],
            )
        )

    inherit_geostore_job = PostgresqlClientJob(
        job_name="inherit from geostore",
        command=["inherit_from_geostore"],
        environment={},
        parents=[job.job_name for job in index_jobs],
    )

    success = execute(
        [
            vector_import_job,
            repair_geometry_job,
            gfw_attribute_job,
            *index_jobs,
            inherit_geostore_job,
        ],
        callback,
    )

    # TODO: evaluate success, set to saved if succeeded, set to failed if not
    print(success)


def _table_source_asset(source_uri: List[str], config_options, callback):
    create_table_job = PostgresqlClientJob(
        job_name="create table", command=["create table"], environment={},
    )

    load_data_jobs: List[Job] = list()

    for i, uri in enumerate(source_uri):
        load_data_jobs.append(
            PostgresqlClientJob(
                job_name=f"load data {i}",
                command=["load_data", uri],
                environment={},
                parents=[create_table_job.job_name],
            )
        )

    gfw_attribute_job = PostgresqlClientJob(
        job_name="enrich gfw attributes",
        command=["enrich_gfw_attributes"],
        environment={},
        parents=[job.job_name for job in load_data_jobs],
    )

    index_jobs: List[Job] = list()

    for index in config_options.indices:
        index_jobs.append(
            PostgresqlClientJob(
                job_name="geom index",
                command=["build_index"],
                environment={},
                parents=[gfw_attribute_job.job_name],
            )
        )

    cluster_jobs: List[Job] = list()
    if config_options.partitions:
        for partition in config_options.partitions:
            index_jobs.append(
                PostgresqlClientJob(
                    job_name="geom index",
                    command=["build_index"],
                    environment={},
                    parents=[job.job_name for job in index_jobs],
                )
            )

    success = execute(
        [
            create_table_job,
            *load_data_jobs,
            gfw_attribute_job,
            *index_jobs,
            *cluster_jobs,
        ],
        callback,
    )

    # TODO: evaluate success, set to saved if succeeded, set to failed if not
    print(success)


def _raster_source_asset(source_type: str, source_uri: List[str], callback):
    pass


async def _inject_file(file_obj: IO, s3_uri: str) -> ChangeLog:
    """
    Upload a file-like object to S3 data lake
    """
    s3 = get_s3_client()
    bucket, path = split_s3_path(s3_uri)

    try:
        s3.upload_fileobj(file_obj, bucket, path)
        status = "success"
        message = f"Injected file {path} into {bucket}"
        detail = None
    except Exception as e:
        status = "failed"
        message = f"Failed to injected file {path} into {bucket}"
        detail = str(e)

    return ChangeLog(
        datetime=datetime.now(), status=status, message=message, detail=detail
    )
