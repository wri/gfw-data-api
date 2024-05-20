from typing import Any, Dict, List
from uuid import UUID

from app.crud import assets
from app.models.enum.assets import AssetType
from app.models.pydantic.change_log import ChangeLog
from app.models.pydantic.creation_options import creation_option_factory
from app.models.pydantic.jobs import PostgresqlClientJob
from app.settings.globals import DATA_LAKE_JOB_QUEUE
from app.tasks import callback_constructor, reader_secrets
from app.tasks.batch import execute
from app.utils.fields import get_field_attributes
from app.utils.path import get_asset_uri


async def static_vector_1x1_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:
    """Create Vector tile cache and NDJSON file as intermediate data."""

    #######################
    # Update asset metadata
    #######################

    creation_options = creation_option_factory(
        AssetType.grid_1x1, input_data["creation_options"]
    )

    field_attributes: List[Dict[str, Any]] = await get_field_attributes(
        dataset, version, creation_options
    )

    grid_1x1_uri = get_asset_uri(dataset, version, AssetType.grid_1x1)

    await assets.update_asset(
        asset_id,
        fields=field_attributes,
    )

    ############################
    # Define jobs
    ############################

    # Create table schema
    command: List[str] = [
        "export_1x1_grid.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-C",
        ",".join([field["name"] for field in field_attributes]),
        "-T",
        grid_1x1_uri,
    ]

    export_1x1_grid = PostgresqlClientJob(
        dataset=dataset,
        job_name="export_1x1_grid",
        job_queue=DATA_LAKE_JOB_QUEUE,
        command=command,
        memory=9000,
        environment=reader_secrets,
        callback=callback_constructor(asset_id),
    )

    #######################
    # execute jobs
    #######################

    log: ChangeLog = await execute([export_1x1_grid])

    return log
