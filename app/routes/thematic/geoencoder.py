from typing import Optional, Any, Dict, List

from fastapi import APIRouter, Query

# from app.routes.datasets.queries import _query_dataset_json

router = APIRouter()


@router.get(
    "/geoencode",
    tags=["Geoencoder"],
    status_code=200,
)
async def geoencode(
    *,
    # country, region, subregion: Tuple[str, str, str] = Depends(geo_hierarchy_dependency),
    admin_source: Optional[str] = Query(
        "GADM",
        description="The source of administrative boundaries to use."
    ),
    admin_version: str = Query(
        None,
        description="Version of the administrative boundaries dataset to use.",
    ),
    country_name: str = Query(
        description="Name of the country to match.",
    ),
    region_name: Optional[str] = Query(
        None,
        description="Name of the region to match.",
    ),
    subregion_name: Optional[str] = Query(
        None,
        description="Name of the subregion to match.",
    ),
):
    """ Look-up administrative boundaries matching a specified country name
    (and possibly region and subregion, if specified).
    """

    return await lookup_admin_boundaries(
        admin_source, admin_version, country_name, region_name, subregion_name

    )


async def lookup_admin_boundaries(
    admin_source: str,
    admin_version: str,
    country_name: str,
    region_name: Optional[str],
    subregion_name: Optional[str]
) -> Dict[str, Any]:
    dataset = "gadm_administrative_boundaries"

    matches = [
            {
                "country": {"id": "HND", "name": "Honduras"},
                "region": {"id": 1, "name": "Atlántida"},
                "subregion": {"id": 4, "name": "Jutiapa"}
            }
        ]

    return {
        "adminSource": admin_source,
        "adminVersion": admin_version,
        "matches": matches
    }
