import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from unidecode import unidecode

from app.crud.versions import get_version, get_version_names
from app.errors import RecordNotFoundError
from app.models.pydantic.responses import Response
from app.routes import VERSION_REGEX
from app.routes.datasets.queries import _query_dataset_json

router = APIRouter()


@router.get(
    "/geoencode",
    tags=["Geoencoder"],
    status_code=200,
)
async def geoencode(
    *,
    admin_source: str = Query(
        "GADM", description="The source of administrative boundaries to use."
    ),
    admin_version: str = Query(
        ...,
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
    unaccent_request: bool = Query(
        True,
        description="Whether or not to unaccent names in request.",
    ),
):
    """Look up administrative boundary IDs matching a specified country name
    (and region name and subregion names, if specified).
    """
    admin_source_to_dataset: Dict[str, str] = {"GADM": "gadm_administrative_boundaries"}

    try:
        dataset = admin_source_to_dataset[admin_source.upper()]
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid admin boundary source. Valid sources:"
                f" {[source for source in admin_source_to_dataset.keys()]}"
            ),
        )

    version_str = "v" + str(admin_version).lstrip("v")

    await version_is_valid(dataset, version_str)

    ensure_admin_hierarchy(region, subregion)

    names: List[str | None] = [country, region, subregion]
    if unaccent_request:
        names = []
        for name in (country, region, subregion):
            if name:
                names.append(unidecode(name))
            else:
                names.append(None)

    sql: str = _admin_boundary_lookup_sql(admin_source, *names)

    json_data: List[Dict[str, Any]] = await _query_dataset_json(
        dataset, version_str, sql, None
    )

    return Response(
        data={
            "adminSource": admin_source,
            "adminVersion": admin_version,
            "matches": json_data,
        }
    )


def ensure_admin_hierarchy(region: str | None, subregion: str | None) -> None:
    if subregion and not region:
        raise HTTPException(
            status_code=400,
            detail="If subregion is specified, region must be specified as well.",
        )


def determine_admin_level(
    country: str | None, region: str | None, subregion: str | None
) -> str:
    if subregion:
        return "2"
    elif region:
        return "1"
    elif country:
        return "0"
    else:  # Shouldn't get here if FastAPI route definition worked
        raise HTTPException(status_code=400, detail="Country MUST be specified.")


def _admin_boundary_lookup_sql(
    dataset: str, country_name: str, region_name: str | None, subregion_name: str | None
) -> str:
    """Generate the SQL required to look up administrative boundary
    IDs by name.
    """
    sql = (
        f"SELECT gid_0, gid_1, gid_2, country, name_1, name_2 FROM {dataset}"
        f" WHERE country='{country_name}'"
    )
    if region_name is not None:
        sql += f" AND name_1='{region_name}'"
    if subregion_name is not None:
        sql += f" AND name_2='{subregion_name}'"

    adm_level = determine_admin_level(country_name, region_name, subregion_name)
    sql += f" AND adm_level='{adm_level}'"

    return sql


async def version_is_valid(
    dataset: str,
    version: str,
) -> None:
    """ """
    if re.match(VERSION_REGEX, version) is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid version name. Version names begin with a 'v' and "
                "consist of one to three integers separated by periods. "
                "eg. 'v1', 'v7.1', 'v4.1.0',  'v20240801'"
            ),
        )

    try:
        _ = await get_version(dataset, version)
    except RecordNotFoundError:
        raise HTTPException(
            status_code=400,
            detail=(
                "Version not found. Existing versions for this dataset "
                f"include {await get_version_names(dataset)}"
            ),
        )
