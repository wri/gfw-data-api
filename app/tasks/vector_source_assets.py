import json
import math
import os
from typing import Any, Dict, List
from uuid import UUID

from ..models.enum.creation_options import VectorDrivers
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import VectorSourceCreationOptions
from ..models.pydantic.jobs import GdalPythonImportJob, Job, PostgresqlClientJob
from ..utils.path import get_layer_name, is_zipped
from . import Callback, callback_constructor, writer_secrets
from .batch import BATCH_DEPENDENCY_LIMIT, execute
from .utils import RingOfLists


async def vector_source_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:

    source_uris: List[str] = input_data["creation_options"].get("source_uri", [])

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

    command = [
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
    ]

    if creation_options.table_schema:
        command += [
            "-m",
            json.dumps(creation_options.dict(by_alias=True)["table_schema"]),
        ]

    create_vector_schema_job = GdalPythonImportJob(
        dataset=dataset,
        job_name="import_vector_data",
        command=command,
        environment=job_env,
        callback=callback,
    )

    load_vector_data_jobs: List[GdalPythonImportJob] = list()
    if creation_options.source_driver == VectorDrivers.csv:
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
                version,
            ]

            for uri in uri_chunk:
                command.append("-s")
                command.append(uri)

            job = GdalPythonImportJob(
                dataset=dataset,
                job_name=f"load_vector_data_layer_{i}",
                command=command,
                parents=[create_vector_schema_job.job_name],
                environment=job_env,
                callback=callback,
                attempt_duration_seconds=creation_options.timeout,
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
                parents: List[str] = [create_vector_schema_job.job_name]
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
                attempt_duration_seconds=creation_options.timeout,
            )
            queue.append(job)
            load_vector_data_jobs.append(job)

        load_data_parents = [queue[-1].job_name for queue in job_queues.all() if queue]

    gfw_attribute_job = PostgresqlClientJob(
        dataset=dataset,
        job_name="enrich_gfw_attributes",
        command=["add_gfw_fields.sh", "-d", dataset, "-v", version],
        parents=load_data_parents,
        environment=job_env,
        callback=callback,
        attempt_duration_seconds=creation_options.timeout,
    )

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
                parents=[gfw_attribute_job.job_name],
                environment=job_env,
                callback=callback,
                attempt_duration_seconds=creation_options.timeout,
            )
        )

    parents = [job.job_name for job in index_jobs]

    cluster_jobs: List[PostgresqlClientJob] = list()
    if creation_options.cluster:
        cluster_jobs.append(
            PostgresqlClientJob(
                dataset=dataset,
                job_name="cluster_table",
                command=[
                    "cluster_table.sh",
                    "-d",
                    dataset,
                    "-v",
                    version,
                    "-C",
                    ",".join(creation_options.cluster.column_names),
                    "-x",
                    creation_options.cluster.index_type,
                ],
                environment=job_env,
                parents=[job.job_name for job in index_jobs],
                callback=callback,
                attempt_duration_seconds=creation_options.timeout,
            )
        )

    parents += [job.job_name for job in cluster_jobs]

    inherit_geostore_jobs = list()
    if creation_options.add_to_geostore:
        inherit_geostore_job = PostgresqlClientJob(
            dataset=dataset,
            job_name="inherit_from_geostore",
            command=["inherit_geostore.sh", "-d", dataset, "-v", version],
            parents=parents,
            environment=job_env,
            callback=callback,
            attempt_duration_seconds=creation_options.timeout,
        )
        inherit_geostore_jobs.append(inherit_geostore_job)

    log: ChangeLog = await execute(
        [
            create_vector_schema_job,
            *load_vector_data_jobs,
            gfw_attribute_job,
            *index_jobs,
            *cluster_jobs,
            *inherit_geostore_jobs,
        ]
    )

    return log
