from typing import List, Optional, Tuple, Union

from pydantic import BaseModel, Field, validator

from ..enum.creation_options import VectorDrivers
from ..enum.versions import VersionStatus
from .base import BaseRecord, StrictBaseModel
from .creation_options import SourceCreationOptions
from .metadata import VersionMetadataIn, VersionMetadataOut, VersionMetadataUpdate
from .responses import Response


class Version(BaseRecord):
    dataset: str
    version: str
    is_latest: bool = False
    is_mutable: bool = False
    metadata: Union[VersionMetadataOut, BaseModel]
    status: VersionStatus = VersionStatus.pending

    assets: List[Tuple[str, str, str]] = Field(
        list(),
        description="List of saved (non-pending and non-failed) assets, with "
        " elements in the form: [asset_type, asset_uri, asset_id]. The list "
        "of assets is sorted by the creation time of each asset."
    )


class VersionCreateIn(StrictBaseModel):
    is_downloadable: Optional[bool] = Field(
        None,
        description="Flag to specify if assets associated with version can be downloaded."
        "If not set, value will default to settings of underlying dataset",
    )
    metadata: Optional[VersionMetadataIn] = Field(
        None,
        description="Version metadata. Version will inherit metadata from dataset. "
        "You will only need to add fields which you want to add or overwrite.",
    )
    creation_options: SourceCreationOptions = Field(
        ...,
        description="Creation option to specify how default asset for version should be created.",
    )


class VersionUpdateIn(StrictBaseModel):
    is_downloadable: Optional[bool] = Field(
        None,
        description="Flag to specify if assets associated with version can be downloaded."
        "If not set, value will default to settings of underlying dataset",
    )
    is_latest: Optional[bool] = Field(
        None,
        description="Indicate if the current version should be tagged `latest`. "
        "This will cause redirects from {dataset}/latest to {dataset}/{current_version}."
        "When tagging a version to `latest` any other version currently tagged `latest` will be untagged.",
    )
    metadata: Optional[VersionMetadataUpdate] = Field(
        None,
        description="Version metadata. Version will inherit metadata from dataset. "
        "You will only need to add fields which you want to add or overwrite.",
    )


class VersionAppendIn(StrictBaseModel):
    source_uri: List[str]
    source_driver: Optional[VectorDrivers] = Field(
        None, description="Driver of source file. Must be an OGR driver"
    )
    layers: Optional[List[str]] = Field(
        None,
        description="List of layer names to append to version. "
        "Only required for .gdb and .gpkg.",
    )

class VersionResponse(Response):
    data: Version
