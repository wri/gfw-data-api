import json
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

TREE_COVER_LOSS_DATASET_NAME = "umd_tree_cover_loss"
TREE_COVER_LOSS_DATASET_VERSION = "v1.11"

TREE_COVER_LOSS_BY_DRIVER_DATASET_NAME = "tsc_tree_cover_loss_drivers"
TREE_COVER_LOSS_BY_DRIVER_DATASET_VERSION = "v2023"

TREE_COVER_DENSTY_DATASET_NAME = "umd_tree_cover_density_2000"
TREE_COVER_LOSS_DENSITY_DATASET_VERSION = "v1.8"


async def compute_tree_cover_loss_by_driver(
    resource_id: UUID, geostore_id: UUID, canopy_cover: int
):
    try:
        logger.info(
            f"Computing tree cover loss by driver for resource {resource_id} with geostore {geostore_id} and canopy cover {canopy_cover}"
        )
        geostore: GeostoreCommon = await get_geostore(geostore_id, GeostoreOrigin.rw)
        query = f"SELECT SUM(area__ha) FROM data WHERE umd_tree_cover_density_2000__threshold >= {canopy_cover} GROUP BY tsc_tree_cover_loss_drivers__driver"

        # TODO right now this is just using latest versions, need
        # to add a way to later to put specific versions in the data environment
        results = await _query_dataset_json(
            TREE_COVER_LOSS_DATASET_NAME,
            TREE_COVER_LOSS_DATASET_VERSION,
            query,
            geostore,
        )

        tcl_by_driver = {
            row["tsc_tree_cover_loss_drivers__driver"]: row["area__ha"]
            for row in results
        }

        resource = TreeCoverLossByDriver(
            treeCoverLossByDriver=tcl_by_driver,
            metadata=_get_metadata(geostore, canopy_cover),
        )

        await _write_resource(resource_id, resource)
    except Exception as e:
        logger.error(e)
        await _write_error(resource_id, str(e))


def _get_metadata(geostore: GeostoreCommon, canopy_cover: int):
    return TreeCoverLossByDriverMetadata(
        geostore_id=geostore.geostore_id,
        canopy_cover=canopy_cover,
        sources=[
            DataMartSource(
                dataset=TREE_COVER_LOSS_DATASET_NAME,
                version=TREE_COVER_LOSS_DATASET_VERSION,
            ),
            DataMartSource(
                dataset=TREE_COVER_LOSS_BY_DRIVER_DATASET_NAME,
                version=TREE_COVER_LOSS_BY_DRIVER_DATASET_VERSION,
            ),
            DataMartSource(
                dataset=TREE_COVER_DENSTY_DATASET_NAME,
                version=TREE_COVER_LOSS_DENSITY_DATASET_VERSION,
            ),
        ],
    )


async def _write_resource(resource_id: UUID, resource: DataMartResource):
    with open(f"/tmp/{resource_id}", "w") as f:
        f.write(resource.model_dump_json())


async def _write_error(resource_id: UUID, error: str):
    error_resource = {"status": "failed", "details": error}
    with open(f"/tmp/{resource_id}", "w") as f:
        f.write(json.dumps(error_resource))
