from typing import Annotated, Any, Dict, List

from fastapi import APIRouter, HTTPException, Query
from unidecode import unidecode

from app.models.pydantic.geoencoder import GeoencoderQueryParams, GeoencoderResponse
from app.routes.datasets.queries import _query_dataset_json
from app.settings.globals import ENV, per_env_admin_boundary_versions

router = APIRouter()


@router.get(
    "/geoencoder", tags=["Geoencoder"], status_code=200, include_in_schema=False
)
async def geoencoder(params: Annotated[GeoencoderQueryParams, Query()]):
    """Look up administrative boundary IDs matching a specified country name
    (and region name and subregion name, if specified).
    """
    admin_source_to_dataset: Dict[str, str] = {"GADM": "gadm_administrative_boundaries"}

    try:
        dataset: str = admin_source_to_dataset[params.admin_source]
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid admin boundary source. Valid sources:"
                f" {[source for source in admin_source_to_dataset.keys()]}"
            ),
        )

    version_str: str = lookup_admin_source_version(
        params.admin_source, params.admin_version
    )

    names: List[str | None] = sanitize_names(
        params.normalize_search, params.country, params.region, params.subregion
    )

    adm_level: int = determine_admin_level(*names)

    sql: str = _admin_boundary_lookup_sql(
        adm_level, params.normalize_search, dataset, *names
    )

    json_data: List[Dict[str, Any]] = await _query_dataset_json(
        dataset, version_str, sql, None
    )

    return GeoencoderResponse(
        **{
            "data": {
                "adminSource": params.admin_source,
                "adminVersion": params.admin_version,
                "matches": [
                    {
                        "country": {
                            "id": match["gid_0"].rsplit("_")[0],
                            "name": match["country"],
                        },
                        "region": {
                            "id": (
                                (match["gid_1"].rsplit("_")[0]).split(".")[1]
                                if adm_level >= 1
                                else None
                            ),
                            "name": match["name_1"] if adm_level >= 1 else None,
                        },
                        "subregion": {
                            "id": (
                                (match["gid_2"].rsplit("_")[0]).split(".")[2]
                                if adm_level >= 2
                                else None
                            ),
                            "name": match["name_2"] if adm_level >= 2 else None,
                        },
                    }
                    for match in json_data
                ],
            }
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
) -> int:
    """Infer the native admin level of a request based on the presence of
    non-empty fields
    """
    if subregion:
        return 2
    elif region:
        return 1
    elif country:
        return 0
    else:  # Shouldn't get here if FastAPI route definition worked
        raise HTTPException(status_code=400, detail="Country MUST be specified.")


def _admin_boundary_lookup_sql(
    adm_level: int,
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


def lookup_admin_source_version(source, version) -> str:
    # The GeoencoderQueryParams validator should have already ensured
    # that the following is safe
    deployed_version_in_data_api = per_env_admin_boundary_versions[ENV][source][version]

    return deployed_version_in_data_api
