from typing import Any, Dict, List

from app.crud import assets
from app.models.pydantic.asset import AssetTaskCreate
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.job import Job, PostgresqlClientJob
from app.models.pydantic.metadata import DatabaseTableMetadata
from app.tasks import get_field_metadata, writer_secrets
from app.tasks.batch import execute


async def table_source_asset(
    dataset,
    version,
    source_uris: List[str],
    config_options,
    metadata: Dict[str, Any],
    callback,
) -> ChangeLog:

    data = AssetTaskCreate(
        asset_type="Database table",
        dataset=dataset,
        version=version,
        asset_uri=f"/{dataset}/{version}/features",
        is_managed=True,
        creation_options=config_options,
        metadata=DatabaseTableMetadata(**metadata),
    )

    new_asset = await assets.create_asset(**data.dict())

    create_table_job = PostgresqlClientJob(
        job_name="create table", command=["create table"], environment=writer_secrets,
    )

    load_data_jobs: List[Job] = list()

    for i, uri in enumerate(source_uris):
        load_data_jobs.append(
            PostgresqlClientJob(
                job_name=f"load data {i}",
                command=["load_data", uri],
                environment=writer_secrets,
                parents=[create_table_job.job_name],
            )
        )

    gfw_attribute_job = PostgresqlClientJob(
        job_name="enrich gfw attributes",
        command=["enrich_gfw_attributes"],
        environment=writer_secrets,
        parents=[job.job_name for job in load_data_jobs],
    )

    index_jobs: List[Job] = list()

    for index in config_options.indices:
        index_jobs.append(
            PostgresqlClientJob(
                job_name="geom index",
                command=["build_index"],
                environment=writer_secrets,
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
                    environment=writer_secrets,
                    parents=[job.job_name for job in index_jobs],
                )
            )

    log: ChangeLog = await execute(
        [
            create_table_job,
            *load_data_jobs,
            gfw_attribute_job,
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

    await assets.update_asset(new_asset.asset_id, status=log.status, metadata=metadata)
    return log
