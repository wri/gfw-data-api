from typing import Optional, Any, Dict, List

from fastapi import APIRouter, Query

from app.routes.datasets.queries import _query_dataset_json


router = APIRouter()


@router.get(
    "/geoencode",
    tags=["Geoencoder"],
    status_code=200,
)
async def geoencode(
    *,
    admin_source: Optional[str] = Query(
        "GADM",
        description="The source of administrative boundaries to use."
    ),
    admin_version: str = Query(
        None,
        description="Version of the administrative boundaries dataset to use.",
    ),
    country: str = Query(
        description="Name of the country to match.",
    ),
    region: Optional[str] = Query(
        None,
        description="Name of the region to match.",
    ),
    subregion: Optional[str] = Query(
        None,
        description="Name of the subregion to match.",
    ),
):
    """ Look-up administrative boundary IDs matching a specified country name
    (and region name and subregion names, if specified).
    """

    return await lookup_admin_boundary_ids(
        admin_source, admin_version, country, region, subregion

    )


async def lookup_admin_boundary_ids(
    admin_source: str,
    admin_version: str,
    country_name: str,
    region_name: Optional[str],
    subregion_name: Optional[str]
) -> Dict[str, Any]:
    source_to_datasets = {
        "gadm": "gadm_administrative_boundaries"
    }

    dataset = source_to_datasets[admin_source.lower()]
    version = f"v{admin_version}"

    base_sql = f"SELECT gid_0, gid_1, gid_2, country, name_1, name_2 FROM {dataset}"
    where_filter:str = f" AND WHERE country='{country_name}'"
    if region_name is not None:
        where_filter += f" AND WHERE region='{region_name}'"
    if subregion_name is not None:
        where_filter += f" AND WHERE subregion='{subregion_name}'"

    json_data: List[Dict[str, Any]] = await _query_dataset_json(
        dataset, version, base_sql + where_filter, None
    )

    return {
        "adminSource": admin_source,
        "adminVersion": admin_version,
        "matches": json_data
    }
