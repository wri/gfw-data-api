import math
import os
from datetime import datetime
from typing import Any, Dict, List, Union
from uuid import UUID

from pydantic import parse_obj_as

from ..models.enum.change_log import ChangeLogStatus
from ..models.enum.creation_options import VectorDrivers
from ..models.enum.sources import RevisionOperation, SourceType
from ..models.pydantic.assets import RevisionHistory
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import (
    AppendCreationOptions,
    DeleteCreationOptions,
    RevisionCreationOptions,
    TableSourceCreationOptions,
    VectorSourceCreationOptions,
)
from ..models.pydantic.jobs import GdalPythonImportJob, Job, PostgresqlClientJob
from ..settings.globals import AURORA_JOB_QUEUE_FAST, CHUNK_SIZE
from ..tasks import Callback, callback_constructor, writer_secrets
from ..tasks.batch import BATCH_DEPENDENCY_LIMIT, execute
from ..utils.path import get_layer_name, is_zipped
from .utils import RingOfLists


async def revision_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:
    creation_options = parse_obj_as(
        Union[AppendCreationOptions, DeleteCreationOptions],
        input_data["creation_options"],
    )

    revision_history = parse_obj_as(
        List[RevisionHistory], input_data["revision_history"]
    )

    source_version = revision_history[0]
    source_creation_options = source_version.creation_options
    source_type = source_creation_options.source_type

    REVISION_ASSET_PIPELINES = {
        (RevisionOperation.append, SourceType.table): revision_append_table_asset,
        (RevisionOperation.append, SourceType.vector): revision_append_vector_asset,
        (RevisionOperation.delete, SourceType.table): revision_delete_table_asset,
        (RevisionOperation.delete, SourceType.vector): revision_delete_vector_asset,
    }

    try:
        op = _get_revision_operation(creation_options)
        log: ChangeLog = await REVISION_ASSET_PIPELINES[(op, source_type)](  # type: ignore
            dataset,
            version,
            source_version.version,
            asset_id,
            creation_options,
            source_creation_options,
        )
        return log
    except KeyError as e:
        raise ValueError(
            f"Revision operation not supported for source type {source_type}: {e}"
        )


def _get_revision_operation(
    creation_options: RevisionCreationOptions,
) -> RevisionOperation:
    if isinstance(creation_options, AppendCreationOptions):
        return RevisionOperation.append
    elif isinstance(creation_options, DeleteCreationOptions):
        return RevisionOperation.delete
    else:
        raise ValueError(f"Invalid creation options for revision: {creation_options}")


async def revision_append_table_asset(
    dataset: str,
    version: str,
    source_version: str,
    asset_id: UUID,
    creation_options: AppendCreationOptions,
    source_creation_options: TableSourceCreationOptions,
) -> ChangeLog:
    if creation_options.source_uri:
        source_uris: List[str] = creation_options.source_uri
    else:
        raise RuntimeError("Empty source uri list")

    callback: Callback = callback_constructor(asset_id)

    job_env: List[Dict[str, Any]] = writer_secrets + [
        {"name": "ASSET_ID", "value": str(asset_id)}
    ]

    # Load data
    load_data_jobs: List[Job] = list()

    # We can break into at most BATCH_DEPENDENCY_LIMIT parallel jobs, otherwise future jobs will hit the dependency
    # limit, so break sources into chunks
    chunk_size = math.ceil(len(source_uris) / BATCH_DEPENDENCY_LIMIT)
    uri_chunks = [
        source_uris[x : x + chunk_size] for x in range(0, len(source_uris), chunk_size)
    ]

    for i, uri_chunk in enumerate(uri_chunks):
        command = [
            "load_tabular_data.sh",
            "-d",
            dataset,
            "-v",
            source_version,
            "-D",
            source_creation_options.delimiter.encode(
                "unicode_escape"
            ).decode(),  # Need to escape special characters such as TAB for batch job payload,
        ]

        for uri in uri_chunk:
            command += ["-s", uri]

        load_data_jobs.append(
            PostgresqlClientJob(
                dataset=dataset,
                job_queue=AURORA_JOB_QUEUE_FAST,
                job_name=f"load_data_{i}",
                command=command,
                environment=job_env,
                callback=callback,
                attempt_duration_seconds=source_creation_options.timeout,
            )
        )

    # Add geometry columns and update geometries
    gfw_attribute_command = [
        "update_gfw_fields_tabular.sh",
        "-d",
        dataset,
        "-v",
        version,
        "--source_version",
        source_version,
    ]

    if source_creation_options.latitude and source_creation_options.longitude:
        gfw_attribute_command += [
            "--lat",
            source_creation_options.latitude,
            "--lng",
            source_creation_options.longitude,
        ]

    gfw_attribute_job: Job = PostgresqlClientJob(
        dataset=dataset,
        job_queue=AURORA_JOB_QUEUE_FAST,
        job_name="update_gfw_fields_tabular",
        command=gfw_attribute_command,
        environment=job_env,
        parents=[job.job_name for job in load_data_jobs],
        callback=callback,
        attempt_duration_seconds=source_creation_options.timeout,
    )

    log: ChangeLog = await execute([*load_data_jobs, gfw_attribute_job])

    return log


async def revision_append_vector_asset(
    dataset: str,
    version: str,
    source_version: str,
    asset_id: UUID,
    creation_options: AppendCreationOptions,
    source_creation_options: VectorSourceCreationOptions,
) -> ChangeLog:
    source_uris: List[str] = source_creation_options.source_uri

    callback: Callback = callback_constructor(asset_id)

    source_uri: str = source_uris[0]
    local_file: str = os.path.basename(source_uri)
    zipped: bool = is_zipped(source_uri)

    if source_creation_options.layers:
        layers = source_creation_options.layers
    else:
        layer = get_layer_name(source_uri)
        layers = [layer]

    job_env = writer_secrets + [{"name": "ASSET_ID", "value": str(asset_id)}]

    load_vector_data_jobs: List[GdalPythonImportJob] = list()
    if source_creation_options.source_driver == VectorDrivers.csv:
        chunk_size = math.ceil(len(source_uris) / BATCH_DEPENDENCY_LIMIT)
        uri_chunks = [
            source_uris[x : x + chunk_size]
            for x in range(0, len(source_uris), chunk_size)
        ]

        for i, uri_chunk in enumerate(uri_chunks):
            command = [
                "load_vector_csv_data.sh",
                "-d",
                dataset,
                "-v",
                source_version,
            ]

            for uri in uri_chunk:
                command.append("-s")
                command.append(uri)

            job = GdalPythonImportJob(
                dataset=dataset,
                job_name=f"load_vector_data_layer_{i}",
                command=command,
                environment=job_env,
                callback=callback,
                attempt_duration_seconds=source_creation_options.timeout,
            )
            load_vector_data_jobs.append(job)

        load_data_parents = [job.job_name for job in load_vector_data_jobs]
    else:
        # AWS Batch jobs can't have more than 20 parents. In case of excessive
        # numbers of layers, create multiple "queues" of dependent jobs, with
        # the next phase being dependent on the last job of each queue.
        num_queues = min(16, len(layers))
        job_queues: RingOfLists = RingOfLists(num_queues)
        for i, layer in enumerate(layers):
            queue = next(job_queues)
            if not queue:
                parents: List[str] = []
            else:
                parents = [queue[-1].job_name]

            job = GdalPythonImportJob(
                dataset=dataset,
                job_name=f"load_vector_data_layer_{i}",
                command=[
                    "load_vector_data.sh",
                    "-d",
                    dataset,
                    "-v",
                    source_version,
                    "-s",
                    source_uri,
                    "-l",
                    layer,
                    "-f",
                    local_file,
                    "-X",
                    str(zipped),
                ],
                parents=parents,
                environment=job_env,
                callback=callback,
                attempt_duration_seconds=source_creation_options.timeout,
            )
            queue.append(job)
            load_vector_data_jobs.append(job)

        load_data_parents = [queue[-1].job_name for queue in job_queues.all() if queue]

    gfw_attribute_job = PostgresqlClientJob(
        dataset=dataset,
        job_name="enrich_gfw_attributes",
        command=["update_gfw_fields_vector.sh", "-d", dataset, "-v", version],
        parents=load_data_parents,
        environment=job_env,
        callback=callback,
        attempt_duration_seconds=source_creation_options.timeout,
    )

    log: ChangeLog = await execute([*load_vector_data_jobs, gfw_attribute_job])

    return log


async def revision_delete_table_asset(*args) -> ChangeLog:
    return ChangeLog(
        date_time=datetime.now(),
        status=ChangeLogStatus.success,
        message="Successfully created delete revision.",
        detail="",
    )


async def revision_delete_vector_asset(*args) -> ChangeLog:
    return ChangeLog(
        date_time=datetime.now(),
        status=ChangeLogStatus.success,
        message="Successfully created delete revision.",
        detail="",
    )


def _chunk_list(data: List[Any], chunk_size: int = CHUNK_SIZE) -> List[List[Any]]:
    """Split list into chunks of fixed size."""
    return [data[x : x + chunk_size] for x in range(0, len(data), chunk_size)]
