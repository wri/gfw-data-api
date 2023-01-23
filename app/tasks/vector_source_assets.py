import json
import math
import os
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from fastapi.encoders import jsonable_encoder

from ..models.enum.creation_options import VectorDrivers
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import FieldType, VectorSourceCreationOptions
from ..models.pydantic.jobs import GdalPythonImportJob, PostgresqlClientJob
from ..utils.path import get_layer_name, is_zipped
from . import Callback, callback_constructor, writer_secrets
from .batch import BATCH_DEPENDENCY_LIMIT, execute
from .utils import RingOfLists, chunk_list


async def _create_vector_schema_job(
    dataset: str,
    version: str,
    source_uri: str,
    layer: str,
    zipped: bool,
    table_schema: Optional[List[FieldType]],
    job_env: List[Dict[str, str]],
    callback: Callback,
) -> GdalPythonImportJob:

    create_schema_command: List[str] = [
        "create_vector_schema.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-s",
        source_uri,
        "-l",
        layer,
        "-f",
        os.path.basename(source_uri),
        "-X",
        str(zipped),
    ]

    if table_schema is not None:
        create_schema_command += [
            "-m",
            json.dumps(jsonable_encoder(table_schema)),
        ]

    return GdalPythonImportJob(
        dataset=dataset,
        job_name="create_vector_schema",
        command=create_schema_command,
        environment=job_env,
        callback=callback,
    )


async def _create_add_gfw_fields_job(
    dataset: str,
    version: str,
    parents: List[str],
    job_env: List[Dict[str, str]],
    callback: Callback,
    attempt_duration_seconds: int,
) -> PostgresqlClientJob:
    return PostgresqlClientJob(
        dataset=dataset,
        job_name="add_gfw_fields",
        command=["add_gfw_fields.sh", "-d", dataset, "-v", version],
        parents=parents,
        environment=job_env,
        callback=callback,
        attempt_duration_seconds=attempt_duration_seconds,
    )


async def _create_load_csv_data_jobs(
    dataset: str,
    version: str,
    source_uris: List[str],
    table_schema: Optional[List[FieldType]],
    parents: List[str],
    job_env: List[Dict[str, str]],
    callback: Callback,
    attempt_duration_seconds: int,
) -> List[GdalPythonImportJob]:

    chunk_size = math.ceil(len(source_uris) / BATCH_DEPENDENCY_LIMIT)
    uri_chunks: List[List[str]] = chunk_list(source_uris, chunk_size)

    load_vector_data_jobs: List[GdalPythonImportJob] = list()

    for i, uri_chunk in enumerate(uri_chunks):
        load_data_command: List[str] = [
            "load_vector_csv_data.sh",
            "-d",
            dataset,
            "-v",
            version,
        ]

        for uri in uri_chunk:
            load_data_command.append("-s")
            load_data_command.append(uri)

        if table_schema is not None:
            load_data_command += [
                "-m",
                json.dumps(jsonable_encoder(table_schema)),
            ]

        load_vector_data_jobs.append(
            GdalPythonImportJob(
                dataset=dataset,
                job_name=f"load_vector_csv_data_{i}",
                command=load_data_command,
                parents=parents,
                environment=job_env,
                callback=callback,
                attempt_duration_seconds=attempt_duration_seconds,
            )
        )
    return load_vector_data_jobs


async def _create_load_other_data_jobs(
    dataset: str,
    version: str,
    source_uri: str,
    layers: List[str],
    zipped: bool,
    table_schema: Optional[List[FieldType]],
    parents: List[str],
    job_env: List[Dict[str, str]],
    callback: Callback,
    attempt_duration_seconds: int,
) -> Tuple[List[GdalPythonImportJob], List[GdalPythonImportJob]]:
    """Create jobs (1 per layer) for a non-CSV vector source file.

    WRT the return value, the first list is the total list of jobs
    created, the second is the last job in each "queue", suitable as the
    parents for any subsequent job(s).
    """
    # AWS Batch jobs can't have more than 20 parents. In case of excessive
    # numbers of layers, create multiple "queues" of dependent jobs, with
    # the next phase being dependent on the last job of each queue.
    num_queues: int = min(BATCH_DEPENDENCY_LIMIT, len(layers))
    job_queues: RingOfLists = RingOfLists(num_queues)

    load_vector_data_jobs: List[GdalPythonImportJob] = list()

    for i, layer in enumerate(layers):
        current_queue = next(job_queues)

        load_data_command: List[str] = [
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
            os.path.basename(source_uri),
            "-X",
            str(zipped),
        ]

        if table_schema is not None:
            load_data_command += [
                "-m",
                json.dumps(jsonable_encoder(table_schema)),
            ]

        load_data_job: GdalPythonImportJob = GdalPythonImportJob(
            dataset=dataset,
            job_name=f"load_vector_data_layer_{i}",
            command=load_data_command,
            parents=[current_queue[-1].job_name] if current_queue else parents,
            environment=job_env,
            callback=callback,
            attempt_duration_seconds=attempt_duration_seconds,
        )
        current_queue.append(load_data_job)
        load_vector_data_jobs.append(load_data_job)

    return load_vector_data_jobs, [queue[-1] for queue in job_queues.all() if queue]


async def vector_source_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:
    callback: Callback = callback_constructor(asset_id)

    creation_options = VectorSourceCreationOptions(**input_data["creation_options"])
    source_uris: List[str] = creation_options.source_uri
    first_source_uri: str = source_uris[0]

    zipped: bool = is_zipped(first_source_uri)

    # FIXME: Shouldn't we by default get all the layers, which might not be
    #  named as the file is?
    if creation_options.layers:
        layers: List[str] = creation_options.layers
    else:
        layers = [get_layer_name(first_source_uri)]

    job_env = writer_secrets + [{"name": "ASSET_ID", "value": str(asset_id)}]

    create_schema_job: GdalPythonImportJob = await _create_vector_schema_job(
        dataset,
        version,
        first_source_uri,
        layers[0],
        zipped,
        creation_options.table_schema,
        job_env=job_env,
        callback=callback,
    )

    add_gfw_fields_job: PostgresqlClientJob = await _create_add_gfw_fields_job(
        dataset,
        version,
        parents=[create_schema_job.job_name],
        job_env=job_env,
        callback=callback,
        attempt_duration_seconds=creation_options.timeout,
    )

    if creation_options.source_driver == VectorDrivers.csv:
        load_data_jobs: List[GdalPythonImportJob] = await _create_load_csv_data_jobs(
            dataset,
            version,
            source_uris,
            creation_options.table_schema,
            parents=[add_gfw_fields_job.job_name],
            job_env=job_env,
            callback=callback,
            attempt_duration_seconds=creation_options.timeout,
        )
        final_load_data_jobs: List[GdalPythonImportJob] = load_data_jobs
    else:
        load_data_jobs, final_load_data_jobs = await _create_load_other_data_jobs(
            dataset,
            version,
            first_source_uri,
            layers,
            zipped,
            creation_options.table_schema,
            parents=[add_gfw_fields_job.job_name],
            job_env=job_env,
            callback=callback,
            attempt_duration_seconds=creation_options.timeout,
        )

    clip_and_reproject_geom_job: PostgresqlClientJob = PostgresqlClientJob(
        dataset=dataset,
        job_name="clip_and_reproject_geom",
        command=["clip_and_reproject_geom.sh", "-d", dataset, "-v", version],
        parents=[job.job_name for job in final_load_data_jobs],
        environment=job_env,
        callback=callback,
        attempt_duration_seconds=creation_options.timeout,
    )

    index_jobs: List[PostgresqlClientJob] = list()
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
                parents=[clip_and_reproject_geom_job.job_name],
                environment=job_env,
                callback=callback,
                attempt_duration_seconds=creation_options.timeout,
            )
        )

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

    geostore_jobs: List[PostgresqlClientJob] = list()
    if creation_options.add_to_geostore:
        inherit_geostore_job = PostgresqlClientJob(
            dataset=dataset,
            job_name="inherit_from_geostore",
            command=["inherit_geostore.sh", "-d", dataset, "-v", version],
            parents=[clip_and_reproject_geom_job.job_name],
            environment=job_env,
            callback=callback,
            attempt_duration_seconds=creation_options.timeout,
        )
        geostore_jobs.append(inherit_geostore_job)

    log: ChangeLog = await execute(
        [
            create_schema_job,
            add_gfw_fields_job,
            *load_data_jobs,
            clip_and_reproject_geom_job,
            *index_jobs,
            *cluster_jobs,
            *geostore_jobs,
        ]
    )

    return log


async def append_vector_source_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:
    callback: Callback = callback_constructor(asset_id)
    job_env = writer_secrets + [{"name": "ASSET_ID", "value": str(asset_id)}]

    creation_options = VectorSourceCreationOptions(**input_data["creation_options"])
    source_uris: List[str] = creation_options.source_uri
    first_source_uri: str = source_uris[0]

    zipped: bool = is_zipped(first_source_uri)

    if creation_options.layers:
        layers: List[str] = creation_options.layers
    else:
        layers = [get_layer_name(first_source_uri)]

    if creation_options.source_driver == VectorDrivers.csv:
        load_data_jobs: List[GdalPythonImportJob] = await _create_load_csv_data_jobs(
            dataset,
            version,
            source_uris,
            creation_options.table_schema,
            parents=[],
            job_env=job_env,
            callback=callback,
            attempt_duration_seconds=creation_options.timeout,
        )
        final_load_data_jobs: List[GdalPythonImportJob] = load_data_jobs
    else:
        load_data_jobs, final_load_data_jobs = await _create_load_other_data_jobs(
            dataset,
            version,
            first_source_uri,
            layers,
            zipped,
            creation_options.table_schema,
            parents=[],
            job_env=job_env,
            callback=callback,
            attempt_duration_seconds=creation_options.timeout,
        )

    clip_and_reproject_geom_job: PostgresqlClientJob = PostgresqlClientJob(
        dataset=dataset,
        job_name="clip_and_reproject_geom",
        command=["clip_and_reproject_geom.sh", "-d", dataset, "-v", version],
        parents=[job.job_name for job in final_load_data_jobs],
        environment=job_env,
        callback=callback,
        attempt_duration_seconds=creation_options.timeout,
    )

    log: ChangeLog = await execute(
        [
            *load_data_jobs,
            clip_and_reproject_geom_job,
        ]
    )

    return log
