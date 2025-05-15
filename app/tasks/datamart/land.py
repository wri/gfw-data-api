import json
import traceback
from enum import Enum
from typing import Dict
from uuid import UUID

from fastapi.logger import logger

import app.crud.datamart as datamart_crud
from app.models.enum.geostore import GeostoreOrigin
from app.models.pydantic.datamart import (
    AnalysisStatus,
    TreeCoverLossByDriverResult,
    TreeCoverLossByDriverUpdate,
)
from app.models.pydantic.geostore import GeostoreCommon
from app.routes.datasets.queries import _query_dataset_json
from app.utils.geostore import get_geostore


class TCL_DRIVERS_DATASET(str, Enum):
    TSC = 'tsc_tree_cover_loss_drivers'
    WRI_GOOGLE = 'wri_google_tree_cover_loss_drivers'


DEFAULT_LAND_DATASET_VERSIONS = {
    "umd_tree_cover_loss": "v1.11",
    TCL_DRIVERS_DATASET.TSC: "v2023",
    "umd_tree_cover_density_2000": "v1.8",
}


TCL_DRIVERS_DATASET_CONFIGS = {
    TCL_DRIVERS_DATASET.TSC: {
        "sql_driver_field": "tsc_tree_cover_loss_drivers__driver",
        "driver_value_map": {
            "Unknown": 0,
            "Permanent agriculture": 1,
            "Commodity driven deforestation": 2,
            "Shifting agriculture": 3,
            "Forestry": 4,
            "Wildfire": 5,
            "Urbanization": 6,
            "Other natural disturbances": 7,
        }
    },
    TCL_DRIVERS_DATASET.WRI_GOOGLE: {
        "sql_driver_field": "wri_google_tree_cover_loss_drivers__category",
        "driver_value_map": {
            "Unknown": 0,
            "Permanent agriculture": 1,
            "Hard commodities": 2,
            "Shifting cultivation": 3,
            "Logging": 4,
            "Wildfire": 5,
            "Settlements & Infrastructure": 6,
            "Other natural disturbances": 7,
        }
    }
}


async def compute_tree_cover_loss_by_driver(
    resource_id: UUID,
    geostore_id: UUID,
    canopy_cover: int,
    dataset_version: Dict[str, str],
):

    try:
        specified_tcl_drivers_config = None
        for tcl_drivers_dataset in (TCL_DRIVERS_DATASET.TSC, TCL_DRIVERS_DATASET.WRI_GOOGLE):
            if tcl_drivers_dataset in dataset_version:
                specified_tcl_drivers_config = TCL_DRIVERS_DATASET_CONFIGS[tcl_drivers_dataset]

        logger.info(
            f"Computing tree cover loss by driver for resource {resource_id} with geostore {geostore_id} and canopy cover {canopy_cover}"
        )
        geostore: GeostoreCommon = await get_geostore(geostore_id, GeostoreOrigin.rw)
        query = f"SELECT SUM(area__ha), SUM(gfw_forest_carbon_gross_emissions__Mg_CO2e) FROM data WHERE umd_tree_cover_density_2000__threshold >= {canopy_cover} GROUP BY umd_tree_cover_loss__year, {specified_tcl_drivers_config['sql_driver_field']}"

        results = await _query_dataset_json(
            "umd_tree_cover_loss",
            dataset_version["umd_tree_cover_loss"],
            query,
            geostore,
            dataset_version,
        )

        for item in results:
            if specified_tcl_drivers_config['sql_driver_field'] in item:
                item["tree_cover_loss_driver"] = item.pop(specified_tcl_drivers_config['sql_driver_field'])

        resource = TreeCoverLossByDriverUpdate(
            result=TreeCoverLossByDriverResult.from_rows(
                rows=results,
                driver_value_map=specified_tcl_drivers_config['driver_value_map']
            ),
            status=AnalysisStatus.saved,
        )
        await datamart_crud.update_result(resource_id, resource)

    except Exception as e:
        # To debug this error in AWS Logs Insights:
        # 1. Go to CloudWatch > Logs Insights
        # 2. Select your log group
        # 3. Run these queries (ALWAYS include type="tree_cover_loss_by_driver" for this workflow):
        #    - Recent failures:
        #      `filter event="analysis_failure" and type="tree_cover_loss_by_driver" | sort @timestamp desc | limit 20`
        #    - Count by error type:
        #      `stats count(*) by error_type | filter type="tree_cover_loss_by_driver"`
        #    - Full error details for a resource:
        #      `filter type="tree_cover_loss_by_driver" and resource_id="YOUR_ID" | display @timestamp, error_details, stack_trace`
        #    - High severity alerts:
        #      `filter severity="high" and type="tree_cover_loss_by_driver" | stats count(*) by bin(1h)`
        # 4. Click "Run query" (results appear in 10-30 seconds)
        logger.error(
            json.dumps({
                "event": "analysis_failure",
                "type": "tree_cover_loss_by_driver",
                "severity": "high",  # Helps with alerting
                "resource_id": str(resource_id),
                "geostore_id": str(geostore_id),
                "canopy_cover": canopy_cover,
                "dataset_version": dataset_version,
                "error_type": e.__class__.__name__,  # e.g., "ValueError", "ConnectionError"
                "error_details": str(e),
                "stack_trace": traceback.format_exc(),
            })
        )
        resource = TreeCoverLossByDriverUpdate(
            status=AnalysisStatus.failed, message=str(e)
        )

        await datamart_crud.update_result(resource_id, resource)
