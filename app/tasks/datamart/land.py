import json
from uuid import UUID

from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.geostore import GeostoreCommon
from app.routes.datasets.queries import _query_dataset_json
from app.utils.geostore import get_geostore


async def compute_tree_cover_loss_by_driver(
    resource_id: UUID, geostore_id: UUID, canopy_cover: int
):
    geostore: GeostoreCommon = await get_geostore(geostore_id, GeostoreOrigin.rw)

    results = await _query_dataset_json(
        "umd_tree_cover_loss",
        "v1.11",
        f"SELECT SUM(area__ha) FROM data WHERE umd_tree_cover_density_2000__threshold = '{canopy_cover}' GROUP BY tsc_tree_cover_loss_by_drivers__driver",
        geostore,
    )

    results_model = {
        "treeCoverLossByDriver": {
            {
                row["tsc_tree_cover_loss_by_drivers__driver"]: row["area__ha"]
                for row in results
            }
        },
        "metadata": {},
    }

    with open(f"/tmp/{resource_id}", "w") as f:
        f.write(str(json.dumps(results_model)))
