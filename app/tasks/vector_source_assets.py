import os
from typing import Any, Dict, List

from app.crud import assets
from app.models.pydantic.assets import AssetTaskCreate
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import VectorSourceCreationOptions
from app.models.pydantic.jobs import GdalPythonImportJob, Job, PostgresqlClientJob
from app.models.pydantic.metadata import DatabaseTableMetadata
from app.tasks import get_field_metadata, writer_secrets
from app.tasks.batch import execute
from app.utils.path import gdal_path


async def vector_source_asset(
    dataset,
    version,
    source_uris: List[str],
    creation_options,
    metadata: Dict[str, Any],
    callback,
) -> ChangeLog:
    assert len(source_uris) == 1, "Vector sources only support one input file"

    options = VectorSourceCreationOptions(**creation_options)

    source_uri: str = gdal_path(source_uris[0], options.zipped)

    if options.layers:
        layers = options.layers
    else:
        layer, _ = os.path.splitext(os.path.basename(source_uri))
        layers = [layer]

    data = AssetTaskCreate(
        asset_type="Database table",
        dataset=dataset,
        version=version,
        asset_uri=f"/{dataset}/{version}/features",
        is_managed=True,
        creation_options=options,
        metadata=DatabaseTableMetadata(**metadata),
    )

    new_asset = await assets.create_asset(**data.dict())

    create_vector_schema_job = GdalPythonImportJob(
        job_name="Import vector data",
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
        ],
    )

    load_vector_data_jobs: List[Job] = list()
    for layer in layers:
        load_vector_data_jobs.append(
            GdalPythonImportJob(
                job_name="Load vector data",
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
                ],
                parents=[create_vector_schema_job.job_name],
                environment=writer_secrets,
            )
        )

    gfw_attribute_job = PostgresqlClientJob(
        job_name="enrich gfw attributes",
        command=["add_gfw_fields.sh", "-d", dataset, "-v", version],
        parents=[job.job_name for job in load_vector_data_jobs],
        environment=writer_secrets,
    )

    index_jobs: List[Job] = list()

    for index in options.indices:
        index_jobs.append(
            PostgresqlClientJob(
                job_name="Create index",
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
        job_name="inherit from geostore",
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

    metadata = new_asset.metadata
    if log.status == "saved":
        field_metadata: List[Dict[str, Any]] = await get_field_metadata(
            dataset, version
        )
        metadata.update(fields=field_metadata)

    await assets.update_asset(new_asset.asset_id, status=log.status, metadata=metadata)
    return log
