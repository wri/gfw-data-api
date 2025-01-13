from enum import Enum
from typing import Dict, Optional

from fastapi.params import Query
from pydantic import root_validator

from app.models.pydantic.base import StrictBaseModel
from app.settings.globals import ENV


class AdministrativeBoundarySource(str, Enum):
    GADM = "GADM"


class GADMBoundaryVersion(str, Enum):
    three_six = "3.6"
    four_one = "4.1"


# TODO: Move this somewhere else
per_env_admin_boundary_versions: Dict[str, Dict[AdministrativeBoundarySource, Dict]] = {
    "test": {
        AdministrativeBoundarySource.GADM: {
            GADMBoundaryVersion.four_one: "v4.1.64",
        }
    },
    "dev": {
        AdministrativeBoundarySource.GADM: {
            GADMBoundaryVersion.four_one: "v4.1.64",
        }
    },
    "staging": {
        AdministrativeBoundarySource.GADM: {
            GADMBoundaryVersion.four_one: "v4.1",
        }
    },
    "production": {
        AdministrativeBoundarySource.GADM: {
            GADMBoundaryVersion.three_six: "v3.6",
            GADMBoundaryVersion.four_one: "v4.1.0",
        }
    },
}


class GeoencoderQueryParams(StrictBaseModel):
    admin_source: AdministrativeBoundarySource = Query(
        AdministrativeBoundarySource.GADM.value,
        description="The source of administrative boundaries to use.",
    )
    admin_version: GADMBoundaryVersion = Query(
        ...,
        description="The version of the administrative boundaries to use.",
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
        description="Whether or not to perform a case- and accent-insensitive search.",
    )

    @root_validator()
    def validate_params(cls, values):
        source = values.get("admin_source")
        assert (
            source is not None
        ), "Must provide admin_source or leave unset for default of GADM"
        version = values.get("admin_version")
        assert version is not None, "Must provide admin_version"

        sources_in_this_env = per_env_admin_boundary_versions.get(ENV)

        versions_of_source_in_this_env = sources_in_this_env.get(source)
        assert versions_of_source_in_this_env is not None, (
            f"Invalid administrative boundary source {source}. Valid "
            f"sources in this environment are {[v for v in sources_in_this_env.keys()]}"
        )

        deployed_version_in_data_api = versions_of_source_in_this_env.get(version)
        assert deployed_version_in_data_api is not None, (
            f"Invalid version {version} for administrative boundary source "
            f"{source}. Valid versions for this source in this environment are "
            f"{[v.value for v in versions_of_source_in_this_env.keys()]}"
        )

        return values
