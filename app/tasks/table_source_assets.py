import json
import math
from typing import Any, Dict, List, Optional
from uuid import UUID

from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import (
    Index,
    Partitions,
    TableSourceCreationOptions,
)
from ..models.pydantic.jobs import Job, PostgresqlClientJob
from ..settings.globals import AURORA_JOB_QUEUE_FAST, CHUNK_SIZE
from ..tasks import Callback, callback_constructor, writer_secrets
from ..tasks.batch import BATCH_DEPENDENCY_LIMIT, execute


async def table_source_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:

    creation_options = TableSourceCreationOptions(**input_data["creation_options"])
    if creation_options.source_uri:
        source_uris: List[str] = creation_options.source_uri
    else:
        raise RuntimeError("No source URI provided.")

    callback: Callback = callback_constructor(asset_id)

    # Create table schema
    command = [
        "create_tabular_schema.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-s",
        source_uris[0],
        "-m",
        json.dumps(creation_options.dict(by_alias=True)["table_schema"]),
    ]
    if creation_options.partitions:
        command.extend(
            [
                "-p",
                creation_options.partitions.partition_type,
                "-c",
                creation_options.partitions.partition_column,
            ]
        )

    job_env: List[Dict[str, Any]] = writer_secrets + [
        {"name": "ASSET_ID", "value": str(asset_id)}
    ]

    create_table_job = PostgresqlClientJob(
        dataset=dataset,
        job_name="create_table",
        command=command,
        environment=job_env,
        callback=callback,
        attempt_duration_seconds=creation_options.timeout,
    )

    # Create partitions
    if creation_options.partitions:
        partition_jobs: List[Job] = _create_partition_jobs(
            dataset,
            version,
            creation_options.partitions,
            [create_table_job.job_name],
            job_env,
            callback,
            creation_options.timeout,
        )
    else:
        partition_jobs = list()

    # Load data
    load_data_jobs: List[Job] = list()

    parents = [create_table_job.job_name]
    parents.extend([job.job_name for job in partition_jobs])

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
            version,
            "-D",
            creation_options.delimiter.encode(
                "unicode_escape"
            ).decode(),  # Need to escape special characters such as TAB for batch job payload
        ]

        for uri in uri_chunk:
            command.append("-s")
            command.append(uri)

        load_data_jobs.append(
            PostgresqlClientJob(
                dataset=dataset,
                job_name=f"load_data_{i}",
                command=command,
                environment=job_env,
                parents=parents,
                callback=callback,
                attempt_duration_seconds=creation_options.timeout,
            )
        )

    add_gfw_fields_command = [
        "add_gfw_fields_tabular.sh",
        "-d",
        dataset,
        "-v",
        version,
    ]

    if creation_options.longitude and creation_options.latitude:
        add_gfw_fields_command += [
            "--lat",
            creation_options.latitude,
            "--lng",
            creation_options.longitude,
        ]

    add_gfw_fields_jobs = [
        PostgresqlClientJob(
            dataset=dataset,
            job_name="add_gfw_fields",
            command=add_gfw_fields_command,
            environment=job_env,
            parents=[job.job_name for job in load_data_jobs],
            callback=callback,
            attempt_duration_seconds=creation_options.timeout,
        )
    ]

    # Add indicies
    index_jobs: List[Job] = list()
    parents = [job.job_name for job in load_data_jobs]
    parents.extend([job.job_name for job in add_gfw_fields_jobs])

    for index in creation_options.indices:
        index_jobs.append(
            PostgresqlClientJob(
                dataset=dataset,
                job_name=f"create_index_{'_'.join(index.column_names)}_{index.index_type}",
                command=[
                    "create_index.sh",
                    "-d",
                    dataset,
                    "-v",
                    version,
                    "-C",
                    ",".join(index.column_names),
                    "-x",
                    index.index_type,
                ],
                parents=parents,
                environment=job_env,
                callback=callback,
                attempt_duration_seconds=creation_options.timeout,
            )
        )

    parents = [job.job_name for job in load_data_jobs]
    parents.extend([job.job_name for job in add_gfw_fields_jobs])
    parents.extend([job.job_name for job in index_jobs])

    if creation_options.cluster:
        cluster_jobs: List[Job] = _create_cluster_jobs(
            dataset,
            version,
            creation_options.partitions,
            creation_options.cluster,
            parents,
            job_env,
            callback,
            creation_options.timeout,
        )
    else:
        cluster_jobs = list()

    log: ChangeLog = await execute(
        [
            create_table_job,
            *partition_jobs,
            *load_data_jobs,
            *add_gfw_fields_jobs,
            *index_jobs,
            *cluster_jobs,
        ]
    )

    return log


async def append_table_source_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:

    creation_options = TableSourceCreationOptions(**input_data["creation_options"])
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
            version,
            "-D",
            creation_options.delimiter.encode(
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
                attempt_duration_seconds=creation_options.timeout,
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
        version,
    ]

    if creation_options.latitude and creation_options.longitude:
        gfw_attribute_command += [
            "--lat",
            creation_options.latitude,
            "--lng",
            creation_options.longitude,
        ]

    gfw_attribute_job: Job = PostgresqlClientJob(
        dataset=dataset,
        job_queue=AURORA_JOB_QUEUE_FAST,
        job_name="update_gfw_fields_tabular",
        command=gfw_attribute_command,
        environment=job_env,
        parents=[job.job_name for job in load_data_jobs],
        callback=callback,
        attempt_duration_seconds=creation_options.timeout,
    )

    log: ChangeLog = await execute([*load_data_jobs, gfw_attribute_job])

    return log


def _create_partition_jobs(
    dataset: str,
    version: str,
    partitions: Partitions,
    parents,
    job_env: List[Dict[str, str]],
    callback: Callback,
    timeout: int,
) -> List[PostgresqlClientJob]:
    """Create partition job depending on the partition type.

    For large partition number, it will break the job into sub jobs
    """

    partition_jobs: List[PostgresqlClientJob] = list()

    if isinstance(partitions.partition_schema, list):
        chunks = _chunk_list(
            [schema.dict(by_alias=True) for schema in partitions.partition_schema]
        )
        for i, chunk in enumerate(chunks):
            partition_schema: str = json.dumps(chunk)
            job: PostgresqlClientJob = _partition_job(
                dataset,
                version,
                partitions.partition_type,
                partition_schema,
                parents,
                i,
                job_env,
                callback,
                timeout,
            )

            partition_jobs.append(job)
    else:

        partition_schema = json.dumps(partitions.partition_schema.dict(by_alias=True))
        job = _partition_job(
            dataset,
            version,
            partitions.partition_type,
            partition_schema,
            parents,
            0,
            job_env,
            callback,
            timeout,
        )
        partition_jobs.append(job)

    return partition_jobs


def _partition_job(
    dataset: str,
    version: str,
    partition_type: str,
    partition_schema: str,
    parents: List[str],
    suffix: int,
    job_env: List[Dict[str, str]],
    callback: Callback,
    timeout: int,
) -> PostgresqlClientJob:
    return PostgresqlClientJob(
        dataset=dataset,
        job_name=f"create_partitions_{suffix}",
        command=[
            "create_partitions.sh",
            "-d",
            dataset,
            "-v",
            version,
            "-p",
            partition_type,
            "-P",
            partition_schema,
        ],
        environment=job_env,
        parents=parents,
        callback=callback,
        attempt_duration_seconds=timeout,
    )


def _create_cluster_jobs(
    dataset: str,
    version: str,
    partitions: Optional[Partitions],
    cluster: Index,
    parents: List[str],
    job_env: List[Dict[str, str]],
    callback: Callback,
    timeout: int,
) -> List[PostgresqlClientJob]:
    # Cluster tables. This is a full lock operation.
    cluster_jobs: List[PostgresqlClientJob] = list()

    if partitions:
        # When using partitions we need to cluster each partition table separately.
        # Playing it save and cluster partition tables one after the other.
        # TODO: Still need to test if we can cluster tables which are part of the same partition concurrently.
        #  this would speed up this step by a lot. Partitions require a full lock on the table,
        #  but I don't know if the lock is aquired for the entire partition or only the partition table.

        if isinstance(partitions.partition_schema, list):
            chunks = _chunk_list(
                [schema.dict(by_alias=True) for schema in partitions.partition_schema]
            )
            for i, chunk in enumerate(chunks):
                partition_schema: str = json.dumps(chunk)
                job: PostgresqlClientJob = _cluster_partition_job(
                    dataset,
                    version,
                    partitions.partition_type,
                    partition_schema,
                    cluster.column_names,
                    cluster.index_type,
                    parents,
                    i,
                    job_env,
                    callback,
                    timeout,
                )
                cluster_jobs.append(job)
                parents = [job.job_name]

        else:
            partition_schema = json.dumps(
                partitions.partition_schema.dict(by_alias=True)
            )

            job = _cluster_partition_job(
                dataset,
                version,
                partitions.partition_type,
                partition_schema,
                cluster.column_names,
                cluster.index_type,
                parents,
                0,
                job_env,
                callback,
                timeout,
            )
            cluster_jobs.append(job)

    else:
        # Without partitions we can cluster the main table directly
        job = PostgresqlClientJob(
            dataset=dataset,
            job_name="cluster_table",
            command=[
                "cluster_table.sh",
                "-d",
                dataset,
                "-v",
                version,
                "-C",
                ",".join(cluster.column_names),
                "-x",
                cluster.index_type,
            ],
            environment=job_env,
            parents=parents,
            callback=callback,
            attempt_duration_seconds=timeout,
        )
        cluster_jobs.append(job)
    return cluster_jobs


def _cluster_partition_job(
    dataset: str,
    version: str,
    partition_type: str,
    partition_schema: str,
    column_names: List[str],
    index_type: str,
    parents: List[str],
    index: int,
    job_env: List[Dict[str, str]],
    callback: Callback,
    timeout: int,
):
    command = [
        "cluster_partitions.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-p",
        partition_type,
        "-P",
        partition_schema,
        "-C",
        ",".join(column_names),
        "-x",
        index_type,
    ]

    return PostgresqlClientJob(
        dataset=dataset,
        job_name=f"cluster_partitions_{index}",
        command=command,
        environment=job_env,
        parents=parents,
        callback=callback,
        attempt_duration_seconds=timeout,
    )


def _chunk_list(data: List[Any], chunk_size: int = CHUNK_SIZE) -> List[List[Any]]:
    """Split list into chunks of fixed size."""
    return [data[x : x + chunk_size] for x in range(0, len(data), chunk_size)]
