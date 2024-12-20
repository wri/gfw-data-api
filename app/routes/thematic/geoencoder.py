from typing import Optional, Any, Dict, List

from fastapi import APIRouter, HTTPException, Query

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
    """ Look up administrative boundary IDs matching a specified country name
    (and region name and subregion names, if specified).
    """
    admin_source_to_dataset = {
        "GADM": "gadm_administrative_boundaries"
    }

    try:
        dataset = admin_source_to_dataset[admin_source.upper()]
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"Invalid admin boundary source. Valid sources: {admin_source_to_dataset.keys()}"
        )

    version_str = "v" + str(admin_version).lstrip("v")

    sql: str = await admin_boundary_lookup_sql(
        admin_source,
        country,
        region,
        subregion
    )

    json_data: List[Dict[str, Any]] = await _query_dataset_json(
        dataset, version_str, sql, None
    )

    return {
        "adminSource": admin_source,
        "adminVersion": admin_version,
        "matches": json_data
    }


async def admin_boundary_lookup_sql(
    dataset: str,
    country_name: str,
    region_name: Optional[str],
    subregion_name: Optional[str]
):
    """Generate the SQL required to look up administrative boundary
    IDs by name.
    """
    sql = (
        f"SELECT gid_0, gid_1, gid_2, country, name_1, name_2 FROM {dataset}"
        f" AND WHERE country='{country_name}'"
    )
    if region_name is not None:
        sql += f" AND WHERE region='{region_name}'"
    if subregion_name is not None:
        sql += f" AND WHERE subregion='{subregion_name}'"

    return sql
