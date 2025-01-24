from typing import List, Optional

from fastapi.params import Query
from pydantic import Field, root_validator

from app.models.pydantic.base import StrictBaseModel
from app.models.pydantic.responses import Response
from app.settings.globals import ENV, per_env_admin_boundary_versions


class GeoencoderQueryParams(StrictBaseModel):
    admin_source: str = Field(
        "GADM",
        description=(
            "The source of administrative boundaries to use "
            "(currently the only valid choice is 'GADM')."
        ),
    )
    admin_version: str = Query(
        ...,
        description=(
            "The version of the administrative boundaries to use "
            "(note that this represents the release of the source dataset, "
            "not the GFW Data API's idea of the version in the database)."
        ),
    )
    country: str = Query(
        ...,
        description="Name of the country to match.",
    )
    region: Optional[str] = Query(
        None,
        description="Name of the region to match.",
    )
    subregion: Optional[str] = Query(
        None,
        description="Name of the subregion to match.",
    )
    normalize_search: bool = Query(
        True,
        description=(
            "Whether or not to perform a case- and " "accent-insensitive search."
        ),
    )

    @root_validator(pre=True)
    def validate_params(cls, values):
        source = values.get("admin_source")
        if source is None:
            raise ValueError(
                "You must provide admin_source or leave unset for the "
                " default value of 'GADM'."
            )

        version = values.get("admin_version")
        if version is None:
            raise ValueError("You must provide an admin_version")

        sources_in_this_env = per_env_admin_boundary_versions[ENV]

        versions_of_source_in_this_env = sources_in_this_env.get(source)
        if versions_of_source_in_this_env is None:
            raise ValueError(
                f"Invalid administrative boundary source {source}. Valid "
                f"sources in this environment are {[v for v in sources_in_this_env.keys()]}"
            )

        deployed_version_in_data_api = versions_of_source_in_this_env.get(version)
        if deployed_version_in_data_api is None:
            raise ValueError(
                f"Invalid version {version} for administrative boundary source "
                f"{source}. Valid versions for this source in this environment are "
                f"{[v for v in versions_of_source_in_this_env.keys()]}"
            )

        return values


class GeoencoderMatchElement(StrictBaseModel):
    id: str | None
    name: str | None


class GeoencoderMatch(StrictBaseModel):
    country: GeoencoderMatchElement
    region: GeoencoderMatchElement
    subregion: GeoencoderMatchElement


class GeoencoderResponseData(StrictBaseModel):
    adminSource: str
    adminVersion: str
    matches: List[GeoencoderMatch]


class GeoencoderResponse(Response):
    data: GeoencoderResponseData
