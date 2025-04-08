from typing import Dict
from uuid import UUID

from fastapi.logger import logger

import app.crud.datamart as datamart_crud
from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.datamart import (
    AnalysisStatus,
    TreeCoverLossByDriverUpdate,
)
from app.models.pydantic.geostore import GeostoreCommon
from app.routes.datasets.queries import _query_dataset_json
from app.utils.geostore import get_geostore

DEFAULT_LAND_DATASET_VERSIONS = {
    "umd_tree_cover_loss": "v1.11",
    "tsc_tree_cover_loss_drivers": "v2023",
    "umd_tree_cover_density_2000": "v1.8",
}


async def compute_tree_cover_loss_by_driver(
    resource_id: UUID,
    geostore_id: UUID,
    canopy_cover: int,
    dataset_version: Dict[str, str],
):

    try:
        logger.info(
            f"Computing tree cover loss by driver for resource {resource_id} with geostore {geostore_id} and canopy cover {canopy_cover}"
        )
        geostore: GeostoreCommon = await get_geostore(geostore_id, GeostoreOrigin.rw)
        query = f"SELECT SUM(area__ha) FROM data WHERE umd_tree_cover_density_2000__threshold >= {canopy_cover} GROUP BY tsc_tree_cover_loss_drivers__driver"

        results = await _query_dataset_json(
            "umd_tree_cover_loss",
            dataset_version["umd_tree_cover_loss"],
            query,
            geostore,
            dataset_version,
        )

        tcl_by_driver = [
            {
                "drivers_type": row["tsc_tree_cover_loss_drivers__driver"],
                "loss_area_ha": row["area__ha"],
            }
            for row in results
        ]

        resource = TreeCoverLossByDriverUpdate(
            result=tcl_by_driver,
            status=AnalysisStatus.saved,
        )
        await datamart_crud.update_result(resource_id, resource)

    except Exception as e:
        logger.error(
            f"Tree cover loss by drivers analysis failed for geostore ${geostore_id} with error: {e}"
        )
        resource = TreeCoverLossByDriverUpdate(
            status=AnalysisStatus.failed, message=str(e)
        )

        await datamart_crud.update_result(resource_id, resource)
