import json
from typing import Any, Awaitable, Dict, List, Optional
from uuid import UUID

from app.application import ContextEngine
from app.crud import assets, tasks

from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import (
    Index,
    Partitions,
    TableSourceCreationOptions,
)
from ..models.pydantic.jobs import Job, PostgresqlClientJob
from ..settings.globals import CHUNK_SIZE
from ..tasks import update_asset_field_metadata, update_asset_status, writer_secrets
from ..tasks.batch import execute


async def table_source_asset(
    dataset: str, version: str, asset_id: UUID, input_data: Dict[str, Any],
) -> ChangeLog:

    source_uris: List[str] = input_data["source_uri"]
    creation_options = TableSourceCreationOptions(**input_data["creation_options"])

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
        json.dumps(creation_options.dict()["table_schema"]),
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
        job_name="create_table", command=command, environment=job_env,
    )

    # Create partitions
    if creation_options.partitions:
        partition_jobs: List[Job] = _create_partition_jobs(
            dataset, version, creation_options.partitions, [create_table_job.job_name]
        )
    else:
        partition_jobs = list()

    # Load data
    load_data_jobs: List[Job] = list()

    parents = [create_table_job.job_name]
    parents.extend([job.job_name for job in partition_jobs])

    for i, uri in enumerate(source_uris):
        load_data_jobs.append(
            PostgresqlClientJob(
                job_name=f"load_data_{i}",
                command=[
                    "load_tabular_data.sh",
                    "-d",
                    dataset,
                    "-v",
                    version,
                    "-s",
                    uri,
                    "-D",
                    creation_options.delimiter.encode(
                        "unicode_escape"
                    ).decode(),  # Need to escape special characters such as TAB for batch job payload
                ],
                environment=job_env,
                parents=parents,
            )
        )

    # Add geometry columns and update geometries
    geometry_jobs: List[Job] = list()
    if creation_options.latitude and creation_options.longitude:
        geometry_jobs.append(
            PostgresqlClientJob(
                job_name="add_point_geometry",
                command=[
                    "add_point_geometry.sh",
                    "-d",
                    dataset,
                    "-v",
                    version,
                    "--lat",
                    creation_options.latitude,
                    "--lng",
                    creation_options.longitude,
                ],
                environment=job_env,
                parents=[job.job_name for job in load_data_jobs],
            ),
        )

    # Add indicies
    index_jobs: List[Job] = list()
    parents = [job.job_name for job in load_data_jobs]
    parents.extend([job.job_name for job in geometry_jobs])

    for index in creation_options.indices:
        index_jobs.append(
            PostgresqlClientJob(
                job_name=f"create_index_{index.column_name}_{index.index_type}",
                command=[
                    "create_index.sh",
                    "-d",
                    dataset,
                    "-v",
                    version,
                    "-c",
                    index.column_name,
                    "-x",
                    index.index_type,
                ],
                parents=parents,
                environment=job_env,
            )
        )

    parents = [job.job_name for job in load_data_jobs]
    parents.extend([job.job_name for job in geometry_jobs])
    parents.extend([job.job_name for job in index_jobs])

    if creation_options.cluster:
        cluster_jobs: List[Job] = _create_cluster_jobs(
            dataset,
            version,
            creation_options.partitions,
            creation_options.cluster,
            parents,
            job_env,
        )
    else:
        cluster_jobs = list()

    async def callback(
        task_id: Optional[UUID], message: Dict[str, Any]
    ) -> Awaitable[None]:
        async with ContextEngine("PUT"):
            if task_id:
                _ = await tasks.create_task(
                    task_id, asset_id=asset_id, change_log=[message]
                )
            return await assets.update_asset(asset_id, change_log=[message])

    log: ChangeLog = await execute(
        [
            create_table_job,
            *partition_jobs,
            *load_data_jobs,
            *geometry_jobs,
            *index_jobs,
            *cluster_jobs,
        ],
        callback,
    )

    await update_asset_field_metadata(
        dataset, version, asset_id,
    )
    await update_asset_status(asset_id, log.status)

    return log


def _create_partition_jobs(
    dataset: str,
    version: str,
    partitions: Partitions,
    parents,
    job_env: List[Dict[str, str]] = [],
) -> List[PostgresqlClientJob]:
    """
    Create partition job depending on the partition type.
    For large partition number, it will break the job into sub jobs
    """

    partition_jobs: List[PostgresqlClientJob] = list()

    if isinstance(partitions.partition_schema, list):
        chunks = _chunk_list([schema.dict() for schema in partitions.partition_schema])
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
            )

            partition_jobs.append(job)
    else:

        partition_schema = json.dumps(partitions.partition_schema.dict())
        job = _partition_job(
            dataset, version, partitions.partition_type, partition_schema, parents,
        )
        partition_jobs.append(job)

    return partition_jobs


def _partition_job(
    dataset: str,
    version: str,
    partition_type: str,
    partition_schema: str,
    parents: List[str],
    suffix: int = 0,
    job_env: List[Dict[str, str]] = [],
) -> PostgresqlClientJob:
    return PostgresqlClientJob(
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
    )


def _create_cluster_jobs(
    dataset: str,
    version: str,
    partitions: Optional[Partitions],
    cluster: Index,
    parents: List[str],
    job_env: List[Dict[str, str]] = [],
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
                [schema.dict() for schema in partitions.partition_schema]
            )
            for i, chunk in enumerate(chunks):
                partition_schema: str = json.dumps(chunk)
                job: PostgresqlClientJob = _cluster_partition_job(
                    dataset,
                    version,
                    partitions.partition_type,
                    partition_schema,
                    cluster.column_name,
                    cluster.index_type,
                    parents,
                    i,
                )
                cluster_jobs.append(job)
                parents = [job.job_name]

        else:
            partition_schema = json.dumps(partitions.partition_schema.dict())

            job = _cluster_partition_job(
                dataset,
                version,
                partitions.partition_type,
                partition_schema,
                cluster.column_name,
                cluster.index_type,
                parents,
            )
            cluster_jobs.append(job)

    else:
        # Without partitions we can cluster the main table directly
        job = PostgresqlClientJob(
            job_name="cluster_table",
            command=[
                "cluster_table.sh",
                "-d",
                dataset,
                "-v",
                version,
                "-c",
                cluster.column_name,
                "-x",
                cluster.index_type,
            ],
            environment=job_env,
            parents=parents,
        )
        cluster_jobs.append(job)
    return cluster_jobs


def _cluster_partition_job(
    dataset: str,
    version: str,
    partition_type: str,
    partition_schema: str,
    column_name: str,
    index_type: str,
    parents: List[str],
    index: int = 0,
    job_env: List[Dict[str, str]] = [],
):
    return PostgresqlClientJob(
        job_name=f"cluster_partitions_{index}",
        command=[
            "cluster_partitions.sh",
            "-d",
            dataset,
            "-v",
            version,
            "-p",
            partition_type,
            "-P",
            partition_schema,
            "-c",
            column_name,
            "-x",
            index_type,
        ],
        environment=job_env,
        parents=parents,
    )


def _chunk_list(data: List[Any], chunk_size: int = CHUNK_SIZE) -> List[List[Any]]:
    """
    Split list into chunks of fixed size.
    """
    return [data[x : x + chunk_size] for x in range(0, len(data), chunk_size)]
