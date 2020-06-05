import json
from typing import Any, Dict, List

from fastapi.logger import logger

from app.application import ContextEngine
from app.crud import assets
from app.models.pydantic.assets import AssetTaskCreate
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import TableSourceCreationOptions
from app.models.pydantic.jobs import Job, PostgresqlClientJob
from app.models.pydantic.metadata import DatabaseTableMetadata
from app.tasks import update_asset_field_metadata, update_asset_status, writer_secrets
from app.tasks.batch import execute


async def table_source_asset(
    dataset,
    version,
    source_uris: List[str],
    creation_options,
    metadata: Dict[str, Any],
    callback,
) -> ChangeLog:
    options = TableSourceCreationOptions(**creation_options)

    # Register asset in database
    data = AssetTaskCreate(
        asset_type="Database table",
        dataset=dataset,
        version=version,
        asset_uri=f"/{dataset}/{version}/features",
        is_managed=True,
        creation_options=options,
        metadata=DatabaseTableMetadata(**metadata),
    )

    async with ContextEngine("PUT"):
        new_asset = await assets.create_asset(**data.dict())

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
        json.dumps(options.dict()["table_schema"]),
    ]
    if options.partitions:
        command.extend(
            [
                "-p",
                options.partitions.partition_type,
                "-c",
                options.partitions.partition_column,
            ]
        )

    create_table_job = PostgresqlClientJob(
        job_name="create_table", command=command, environment=writer_secrets,
    )

    # Create partitions
    partition_jobs: List[Job] = list()
    if options.partitions:

        if isinstance(options.partitions.partition_schema, list):
            partition_schema: str = json.dumps(
                [schema.dict() for schema in options.partitions.partition_schema]
            )
        else:
            partition_schema = json.dumps(options.partitions.partition_schema.dict())

        partition_job = PostgresqlClientJob(
            job_name="create_partitions",
            command=[
                "create_partitions.sh",
                "-d",
                dataset,
                "-v",
                version,
                "-p",
                options.partitions.partition_type,
                "-P",
                partition_schema,
            ],
            environment=writer_secrets,
            parents=[create_table_job.job_name],
        )
        partition_jobs.append(partition_job)

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
                    options.delimiter,
                ],
                environment=writer_secrets,
                parents=parents,
            )
        )

    # Add geometry columns and update geometries
    geometry_jobs: List[Job] = list()
    if options.latitude and options.longitude:
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
                    options.latitude,
                    "--lng",
                    options.longitude,
                ],
                environment=writer_secrets,
                parents=[job.job_name for job in load_data_jobs],
            ),
        )

    # Add indicies
    index_jobs: List[Job] = list()
    parents = [job.job_name for job in load_data_jobs]
    parents.extend([job.job_name for job in geometry_jobs])

    for index in options.indices:
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
                environment=writer_secrets,
            )
        )

    # Cluster tables. This is a full lock operation.
    cluster_jobs: List[Job] = list()

    parents = [job.job_name for job in load_data_jobs]
    parents.extend([job.job_name for job in geometry_jobs])
    parents.extend([job.job_name for job in index_jobs])

    if options.cluster and options.partitions:
        # When using partitions we need to cluster each partition table separately.
        # Playing it save and cluster partition tables one after the other.
        # TODO: Still need to test if we can cluster tables which are part of the same partition concurrently.
        #  this would speed up this step by a lot. Partitions require a full lock on the table,
        #  but I don't know if the lock is aquired for the entire partition or only the partition table.

        if isinstance(options.partitions.partition_schema, list):
            partition_schema = json.dumps(
                [schema.dict() for schema in options.partitions.partition_schema]
            )
        else:
            partition_schema = json.dumps(options.partitions.partition_schema.dict())

        cluster_jobs.append(
            PostgresqlClientJob(
                job_name="cluster_partitions",
                command=[
                    "cluster_partitions.sh",
                    "-d",
                    dataset,
                    "-v",
                    version,
                    "-p",
                    options.partitions.partition_type,
                    "-P",
                    partition_schema,
                    "-c",
                    options.cluster.column_name,
                    "-x",
                    options.cluster.index_type,
                ],
                environment=writer_secrets,
                parents=parents,
            )
        )
    elif options.cluster:
        # Without partitions we can cluster the main table directly
        cluster_jobs.append(
            PostgresqlClientJob(
                job_name="cluster_table",
                command=[
                    "cluster_table.sh",
                    "-d",
                    dataset,
                    "-v",
                    version,
                    "-c",
                    options.cluster.column_name,
                    "-x",
                    options.cluster.index_type,
                ],
                environment=writer_secrets,
                parents=parents,
            )
        )

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
        dataset, version, new_asset.asset_id,
    )
    await update_asset_status(new_asset.asset_id, log.status)

    return log
