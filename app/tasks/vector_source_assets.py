import os
from typing import Any, Dict, List
from uuid import UUID

from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import VectorSourceCreationOptions
from ..models.pydantic.jobs import GdalPythonImportJob, Job, PostgresqlClientJob
from ..utils.path import get_layer_name, is_zipped
from . import Callback, callback_constructor, writer_secrets
from .batch import execute
from .utils import RingOfLists


async def vector_source_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:

    source_uris: List[str] = input_data["creation_options"].get("source_uri", [])

    if len(source_uris) != 1:
        raise AssertionError("Vector sources require one and only one input file")

    creation_options = VectorSourceCreationOptions(**input_data["creation_options"])
    callback: Callback = callback_constructor(asset_id)

    source_uri: str = source_uris[0]
    local_file: str = os.path.basename(source_uri)
    zipped: bool = is_zipped(source_uri)

    if creation_options.layers:
        layers = creation_options.layers
    else:
        layer = get_layer_name(source_uri)
        layers = [layer]

    job_env = writer_secrets + [{"name": "ASSET_ID", "value": str(asset_id)}]

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
            "-X",
            str(zipped),
        ],
        environment=job_env,
        callback=callback,
    )

    # AWS Batch jobs can't have more than 20 parents. In case of excessive
    # numbers of layers, create multiple "queues" of dependent jobs, with
    # the next phase being dependent on the last job of each queue.
    num_queues = min(16, len(layers))
    job_queues: RingOfLists = RingOfLists(num_queues)
    for layer in layers:
        queue = next(job_queues)
        if not queue:
            parents: List[str] = [create_vector_schema_job.job_name]
        else:
            parents = [queue[-1].job_name]
        queue.append(
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
                    "-X",
                    str(zipped),
                ],
                parents=parents,
                environment=job_env,
                callback=callback,
            )
        )
    # Flatten list of all load vector data jobs to pass to execute, later
    load_vector_data_jobs = [item for sublist in job_queues.all() for item in sublist]

    gfw_attribute_job = PostgresqlClientJob(
        job_name="enrich_gfw_attributes",
        command=["add_gfw_fields.sh", "-d", dataset, "-v", version],
        parents=[queue[-1].job_name for queue in job_queues.all()],
        environment=job_env,
        callback=callback,
    )

    index_jobs: List[Job] = list()

    for index in creation_options.indices:
        index_jobs.append(
            PostgresqlClientJob(
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
                parents=[gfw_attribute_job.job_name],
                environment=job_env,
                callback=callback,
            )
        )

    inherit_geostore_jobs = list()
    if creation_options.add_to_geostore:

        inherit_geostore_job = PostgresqlClientJob(
            job_name="inherit_from_geostore",
            command=["inherit_geostore.sh", "-d", dataset, "-v", version],
            parents=[job.job_name for job in index_jobs],
            environment=job_env,
            callback=callback,
        )
        inherit_geostore_jobs.append(inherit_geostore_job)

    log: ChangeLog = await execute(
        [
            create_vector_schema_job,
            *load_vector_data_jobs,
            gfw_attribute_job,
            *index_jobs,
            *inherit_geostore_jobs,
        ]
    )

    return log
