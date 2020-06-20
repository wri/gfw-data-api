import os
from typing import Any, Awaitable, Callable, Dict, List
from uuid import UUID

from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import VectorSourceCreationOptions
from ..models.pydantic.jobs import GdalPythonImportJob, Job, PostgresqlClientJob
from . import update_asset_field_metadata, update_asset_status, writer_secrets
from .batch import execute


async def vector_source_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
    callback: Callable[[Dict[str, Any]], Awaitable[None]],  # TODO delete
) -> ChangeLog:

    source_uris: List[str] = input_data["source_uri"]

    if len(source_uris) != 1:
        raise AssertionError("Vector sources only support one input file")

    creation_options = VectorSourceCreationOptions(**input_data["creation_options"])

    # source_uri: str = gdal_path(source_uris[0], options.zipped)
    source_uri = source_uris[0]
    local_file = os.path.basename(source_uri)

    if creation_options.layers:
        layers = creation_options.layers
    else:
        layer, _ = os.path.splitext(os.path.basename(source_uri))
        layers = [layer]

    create_vector_schema_job = GdalPythonImportJob(
        job_name="import_vector_data",
        command=[
            "create_vector_schema.sh",
            "-d",
            dataset,
            "-v",
            version,
            "-s",
            source_uri,
            "-l",
            layers[0],
            "-f",
            local_file,
        ],
        environment=writer_secrets,
    )

    load_vector_data_jobs: List[Job] = list()
    for layer in layers:
        load_vector_data_jobs.append(
            GdalPythonImportJob(
                job_name="load_vector_data",
                command=[
                    "load_vector_data.sh",
                    "-d",
                    dataset,
                    "-v",
                    version,
                    "-s",
                    source_uri,
                    "-l",
                    layer,
                    "-f",
                    local_file,
                ],
                parents=[create_vector_schema_job.job_name],
                environment=writer_secrets,
            )
        )

    gfw_attribute_job = PostgresqlClientJob(
        job_name="enrich_gfw_attributes",
        command=["add_gfw_fields.sh", "-d", dataset, "-v", version],
        parents=[job.job_name for job in load_vector_data_jobs],
        environment=writer_secrets,
    )

    index_jobs: List[Job] = list()

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
                parents=[gfw_attribute_job.job_name],
                environment=writer_secrets,
            )
        )

    inherit_geostore_job = PostgresqlClientJob(
        job_name="inherit_from_geostore",
        command=["inherit_geostore.sh", "-d", dataset, "-v", version],
        parents=[job.job_name for job in index_jobs],
        environment=writer_secrets,
    )

    log: ChangeLog = await execute(
        [
            create_vector_schema_job,
            *load_vector_data_jobs,
            gfw_attribute_job,
            *index_jobs,
            inherit_geostore_job,
        ],
        callback,
    )

    await update_asset_field_metadata(
        dataset, version, asset_id,
    )
    await update_asset_status(asset_id, log.status)

    return log
