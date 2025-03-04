from typing import Dict
from uuid import UUID

from fastapi.logger import logger

from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.datamart import (
    DataMartResource,
    DataMartSource,
    TreeCoverLossByDriver,
    TreeCoverLossByDriverMetadata,
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
            DEFAULT_LAND_DATASET_VERSIONS["umd_tree_cover_loss"],
            query,
            geostore,
            dataset_version,
        )

        tcl_by_driver = {
            row["tsc_tree_cover_loss_drivers__driver"]: row["area__ha"]
            for row in results
        }

        resource = TreeCoverLossByDriver(
            treeCoverLossByDriver=tcl_by_driver,
            metadata=_get_metadata(geostore, canopy_cover, dataset_version),
        )

        await _write_resource(resource_id, resource)
    except Exception as e:
        logger.error(e)
        await _write_error(resource_id, str(e))


def _get_metadata(
    geostore: GeostoreCommon, canopy_cover: int, dataset_version: Dict[str, str]
):
    sources = [
        DataMartSource(dataset=dataset, version=version)
        for dataset, version in dataset_version.items()
    ]
    return TreeCoverLossByDriverMetadata(
        geostore_id=geostore.geostore_id,
        canopy_cover=canopy_cover,
        sources=sources,
    )


async def _write_resource(resource_id: UUID, resource: DataMartResource):
    with open(f"/tmp/{resource_id}", "w") as f:
        f.write(resource.model_dump_json())


async def _write_error(resource_id: UUID, error: str):
    error_resource = DataMartResource(status="failed", details=error)
    with open(f"/tmp/{resource_id}", "w") as f:
        f.write(error_resource.model_dump_json())
