import json
import math
from typing import Any, Dict, List, Optional
from uuid import UUID

from ..models.enum.creation_options import ConstraintType
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import (
    Index,
    Partitions,
    TableSourceCreationOptions,
)
from ..models.pydantic.jobs import Job, PostgresqlClientJob
from ..settings.globals import AURORA_JOB_QUEUE_FAST
from ..tasks import Callback, callback_constructor, writer_secrets
from ..tasks.batch import BATCH_DEPENDENCY_LIMIT, execute
from .utils import chunk_list


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
    ]
    if creation_options.table_schema:
        command.extend(
            [
                "-m",
                json.dumps(creation_options.dict(by_alias=True)["table_schema"]),
            ]
        )
    if creation_options.partitions:
        command.extend(
            [
                "-p",
                creation_options.partitions.partition_type,
                "-c",
                creation_options.partitions.partition_column,
            ]
        )
    if creation_options.constraints:
        unique_constraint_columns = []
        for constraint in creation_options.constraints:
            if constraint.constraint_type == ConstraintType.unique:
                unique_constraint_columns += constraint.column_names

        command.extend(["-u", ",".join(unique_constraint_columns)])

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
    partition_jobs: List[Job] = list()
    if creation_options.partitions:
        partition_jobs = _create_partition_jobs(
            dataset,
            version,
            creation_options.partitions,
            [create_table_job.job_name],
            job_env,
            callback,
            creation_options.timeout,
        )

    # Add geometry columns
    geometry_jobs: List[Job] = list()
    if creation_options.latitude and creation_options.longitude:
        geometry_jobs.append(
            PostgresqlClientJob(
                dataset=dataset,
                job_name="add_point_geometry",
                command=[
                    "add_point_geometry_fields.sh",
                    "-d",
                    dataset,
                    "-v",
                    version,
                ],
                environment=job_env,
                parents=[create_table_job.job_name]
                + [job.job_name for job in partition_jobs],
                callback=callback,
                attempt_duration_seconds=creation_options.timeout,
            ),
        )

    # Load data and fill geometry fields if lat, lng specified
    load_data_jobs: List[Job] = list()

    load_data_job_parents = [
        create_table_job.job_name,
        *[job.job_name for job in geometry_jobs + partition_jobs],
    ]

    # We can break into at most BATCH_DEPENDENCY_LIMIT parallel jobs
    # (otherwise future jobs will hit the dependency limit) so break
    # source_uris into chunks
    chunk_size = math.ceil(len(source_uris) / BATCH_DEPENDENCY_LIMIT)
    uri_chunks = chunk_list(source_uris, chunk_size)

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

        if creation_options.latitude and creation_options.longitude:
            command += [
                "--lat",
                creation_options.latitude,
                "--lng",
                creation_options.longitude,
            ]

        load_data_jobs.append(
            PostgresqlClientJob(
                dataset=dataset,
                job_name=f"load_tabular_data_{i}",
                command=command,
                environment=job_env,
                parents=load_data_job_parents,
                callback=callback,
                attempt_duration_seconds=creation_options.timeout,
            )
        )

    # Add indices
    index_jobs: List[Job] = list()
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
                parents=[job.job_name for job in load_data_jobs],
                environment=job_env,
                callback=callback,
                attempt_duration_seconds=creation_options.timeout,
            )
        )

    # Add clusters
    cluster_jobs: List[Job] = list()
    if creation_options.cluster:
        cluster_jobs = _create_cluster_jobs(
            dataset,
            version,
            creation_options.partitions,
            creation_options.cluster,
            [job.job_name for job in load_data_jobs + index_jobs],
            job_env,
            callback,
            creation_options.timeout,
        )

    log: ChangeLog = await execute(
        [
            create_table_job,
            *partition_jobs,
            *load_data_jobs,
            *geometry_jobs,
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

    # We can break into at most BATCH_DEPENDENCY_LIMIT parallel jobs
    # (otherwise future jobs will hit the dependency limit)
    # so break source_uris into chunks
    chunk_size = math.ceil(len(source_uris) / BATCH_DEPENDENCY_LIMIT)
    uri_chunks = chunk_list(source_uris, chunk_size)

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

        if creation_options.latitude and creation_options.longitude:
            command += [
                "--lat",
                creation_options.latitude,
                "--lng",
                creation_options.longitude,
            ]

        load_data_jobs.append(
            PostgresqlClientJob(
                dataset=dataset,
                job_queue=AURORA_JOB_QUEUE_FAST,
                job_name=f"load_tabular_data_{i}",
                command=command,
                environment=job_env,
                callback=callback,
                attempt_duration_seconds=creation_options.timeout,
            )
        )

    log: ChangeLog = await execute(load_data_jobs)

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
        chunks = chunk_list(
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
        # Play it safe and cluster partition tables one after the other.
        # TODO: Still need to test if we can cluster tables which are part of the same partition concurrently.
        #  this would speed up this step by a lot. Partitions require a full lock on the table,
        #  but I don't know if the lock is acquired for the entire partition or only the partition table.

        if isinstance(partitions.partition_schema, list):
            chunks = chunk_list(
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
