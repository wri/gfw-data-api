from typing import Any, Dict, List
from uuid import UUID

from ..crud import assets
from ..models.enum.creation_options import VectorDrivers
from ..models.pydantic.assets import AssetType
from ..models.pydantic.change_log import ChangeLog
from ..models.pydantic.creation_options import creation_option_factory
from ..models.pydantic.jobs import GdalPythonExportJob
from ..utils.fields import get_field_attributes
from ..utils.path import get_asset_uri
from . import callback_constructor, reader_secrets
from .batch import execute


async def static_vector_file_asset(
    dataset: str,
    version: str,
    asset_id: UUID,
    input_data: Dict[str, Any],
) -> ChangeLog:
    """Export Vector data to different file formats."""

    options = {
        AssetType.shapefile: {
            "driver": VectorDrivers.shp,
            "extension": "shp",
            "zipped": True,
        },
        AssetType.geopackage: {
            "driver": VectorDrivers.gpkg,
            "extension": "gpkg",
            "zipped": False,
        },
    }

    #######################
    # Update asset metadata
    #######################

    asset_type = input_data["asset_type"]

    creation_options = creation_option_factory(
        asset_type, input_data["creation_options"]
    )

    field_attributes: List[Dict[str, Any]] = await get_field_attributes(
        dataset, version, creation_options
    )

    await assets.update_asset(
        asset_id,
        fields=field_attributes,
    )

    uri = get_asset_uri(dataset, version, asset_type)

    ############################
    # Define jobs
    ############################

    # Export Vector Data
    command: List[str] = [
        "export_vector_data.sh",
        "-d",
        dataset,
        "-v",
        version,
        "-f",
        f"{dataset}_{version}.{options[asset_type]['extension']}",
        "-F",
        options[asset_type]["driver"],
        "-T",
        uri,
        "-C",
        ",".join([field["name"] for field in field_attributes]),
        "-X",
        str(options[asset_type]["zipped"]),
    ]

    export_shp = GdalPythonExportJob(
        dataset=dataset,
        job_name="export_shp",
        command=command,
        environment=reader_secrets,
        callback=callback_constructor(asset_id),
    )

    #######################
    # execute jobs
    #######################

    log: ChangeLog = await execute([export_shp])

    return log
