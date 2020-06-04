import json
from typing import Any, Dict, List

from app.application import ContextEngine
from app.crud import assets
from app.models.pydantic.assets import AssetTaskCreate
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import TableSourceCreationOptions
from app.models.pydantic.jobs import Job, PostgresqlClientJob
from app.models.pydantic.metadata import DatabaseTableMetadata
from app.tasks import get_field_metadata, writer_secrets
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

    # TODO: We can do better than this!
    #  ideally this would be offloaded into a single batch job.
    #  Will need to figure out how to best pass the long lists to the container
    #  Also need to sanitize input data to avoid SQL injection!!
    partition_jobs: List[Job] = list()
    if options.partitions:
        if options.partitions.partition_type == "hash" and isinstance(
            options.partitions.partition_schema, int
        ):
            for i in range(options.partitions.partition_schema):
                command = [
                    "psql",
                    "-c",
                    f'CREATE TABLE "{dataset}"."{version}_{i}" PARTITION OF "{dataset}"."{version}" FOR VALUES WITH (MODULUS {options.partitions.partition_schema}, REMAINDER {i})',
                ]
                partition_jobs.append(
                    PostgresqlClientJob(
                        job_name=f"create_partition_{i}",
                        command=command,
                        environment=writer_secrets,
                        parents=[create_table_job.job_name],
                    )
                )
        elif options.partitions.partition_type == "list" and isinstance(
            options.partitions.partition_schema, dict
        ):
            for key in options.partitions.partition_schema.keys():
                command = [
                    "psql",
                    "-c",
                    f'CREATE TABLE "{dataset}"."{version}_{key}" PARTITION OF "{dataset}"."{version}" FOR VALUES IN {tuple(options.partitions.partition_schema[key])}',
                ]
                partition_jobs.append(
                    PostgresqlClientJob(
                        job_name=f"create_partition_{key}",
                        command=command,
                        environment=writer_secrets,
                        parents=[create_table_job.job_name],
                    )
                )

        elif options.partitions.partition_type == "range" and isinstance(
            options.partitions.partition_schema, dict
        ):
            for key in options.partitions.partition_schema.keys():
                command = [
                    "psql",
                    "-c",
                    f"""CREATE TABLE "{dataset}"."{version}_{key}" PARTITION OF "{dataset}"."{version}" FOR VALUES FROM ('{options.partitions.partition_schema[key][0]}') TO ('{options.partitions.partition_schema[key][1]}')""",
                ]
                partition_jobs.append(
                    PostgresqlClientJob(
                        job_name=f"create_partition_{key}",
                        command=command,
                        environment=writer_secrets,
                        parents=[create_table_job.job_name],
                    )
                )
        else:
            NotImplementedError(
                "The Partition type and schema combination is not supported"
            )

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

    # TODO:
    # Check if possible to break this down by partition tables
    # Batch script should function the same. Instead of version name pass partition table name
    cluster_jobs: List[Job] = list()

    parents = [job.job_name for job in load_data_jobs]
    parents.extend([job.job_name for job in geometry_jobs])
    parents.extend([job.job_name for job in index_jobs])

    if options.cluster:
        cluster_jobs.append(
            PostgresqlClientJob(
                job_name="cluster",
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

    metadata = new_asset.metadata
    if log.status == "saved":
        field_metadata: List[Dict[str, Any]] = await get_field_metadata(
            dataset, version
        )
        metadata.update(fields=field_metadata)

    async with ContextEngine("PUT"):
        await assets.update_asset(
            new_asset.asset_id, status=log.status, metadata=metadata
        )
    return log
