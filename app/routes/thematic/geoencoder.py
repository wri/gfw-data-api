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
    normalize_search: bool = Query(
        True,
        description="Whether or not to perform a case- and accent-insensitive search.",
    ),
):
    """Look up administrative boundary IDs matching a specified country name
    (and region name and subregion names, if specified).
    """
    admin_source_to_dataset: Dict[str, str] = {"GADM": "gadm_administrative_boundaries"}

    try:
        dataset: str = admin_source_to_dataset[admin_source.upper()]
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid admin boundary source. Valid sources:"
                f" {[source for source in admin_source_to_dataset.keys()]}"
            ),
        )

    version_str: str = "v" + str(admin_version).lstrip("v")

    await version_is_valid(dataset, version_str)

    names: List[str | None] = sanitize_names(
        normalize_search, country, region, subregion
    )

    adm_level: str = determine_admin_level(*names)

    sql: str = _admin_boundary_lookup_sql(
        adm_level, normalize_search, admin_source, *names
    )

    json_data: List[Dict[str, Any]] = await _query_dataset_json(
        dataset, version_str, sql, None
    )

    return Response(
        data={
            "adminSource": admin_source,
            "adminVersion": admin_version,
            "matches": [
                {
                    "country": {
                        "id": match["gid_0"].rsplit("_")[0],
                        "name": match["country"],
                    },
                    "region": {
                        "id": (
                            match["gid_1"].rsplit("_")[0]
                            if int(adm_level) >= 1
                            else None
                        ),
                        "name": match["name_1"] if int(adm_level) >= 1 else None,
                    },
                    "subregion": {
                        "id": (
                            match["gid_2"].rsplit("_")[0]
                            if int(adm_level) >= 2
                            else None
                        ),
                        "name": match["name_2"] if int(adm_level) >= 2 else None,
                    },
                }
                for match in json_data
            ],
        }
    )


def sanitize_names(
    normalize_search: bool,
    country: str | None,
    region: str | None,
    subregion: str | None,
) -> List[str | None]:
    """Turn any empty strings into Nones, enforces the admin level hierarchy,
    and optionally unaccents and decapitalizes names.
    """
    names = []

    if subregion and not region:
        raise HTTPException(
            status_code=400,
            detail="If subregion is specified, region must be specified as well.",
        )

    for name in (country, region, subregion):
        if name and normalize_search:
            names.append(unidecode(name).lower())
        elif name:
            names.append(name)
        else:
            names.append(None)
    return names


def determine_admin_level(
    country: str | None, region: str | None, subregion: str | None
) -> str:
    """Infer the native admin level of a request based on the presence of
    non-empty fields
    """
    if subregion:
        return "2"
    elif region:
        return "1"
    elif country:
        return "0"
    else:  # Shouldn't get here if FastAPI route definition worked
        raise HTTPException(status_code=400, detail="Country MUST be specified.")


def _admin_boundary_lookup_sql(
    adm_level: str,
    normalize_search: bool,
    dataset: str,
    country_name: str,
    region_name: str | None,
    subregion_name: str | None,
) -> str:
    """Generate the SQL required to look up administrative boundary
    IDs by name.
    """
    name_fields: List[str] = ["country", "name_1", "name_2"]
    if normalize_search:
        match_name_fields = [name_field + "_normalized" for name_field in name_fields]
    else:
        match_name_fields = name_fields

    sql = (
        f"SELECT gid_0, gid_1, gid_2, {name_fields[0]}, {name_fields[1]}, {name_fields[2]}"
        f" FROM {dataset} WHERE {match_name_fields[0]}='{country_name}'"
    )
    if region_name is not None:
        sql += f" AND {match_name_fields[1]}='{region_name}'"
    if subregion_name is not None:
        sql += f" AND {match_name_fields[2]}='{subregion_name}'"

    sql += f" AND adm_level='{adm_level}'"

    return sql


async def version_is_valid(
    dataset: str,
    version: str,
) -> None:
    """Validate a version string for a given dataset."""
    # Note: At some point I intend to change the version validator to
    # use messaging like this. However, that is out of scope for the
    # current ticket, and I want to see what people think, so I'll
    # keep it here for now.
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
                f"include {[v[0] for v in await get_version_names(dataset)]}"
                # FIXME: Maybe change get_version_names to do unpacking? ^
            ),
        )
