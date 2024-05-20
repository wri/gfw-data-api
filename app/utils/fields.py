from typing import Any, Dict, List

from ..crud import assets, metadata as metadata_crud
from ..models.orm.assets import Asset as ORMAsset
from ..models.pydantic.creation_options import CreationOptions


async def get_field_attributes(
    dataset: str, version: str, creation_options: CreationOptions
) -> List[Dict[str, Any]]:
    """Get field attribute list from creation options.

    If no attribute list provided, use all fields from DB table, marked
    as `is_feature_info`. Otherwise compare to provide list with
    available fields and use intersection.
    """

    default_asset: ORMAsset = await assets.get_default_asset(dataset, version)
    fields = await metadata_crud.get_asset_fields_dicts(default_asset)

    field_attributes: List[Dict[str, Any]] = [
        field for field in fields if field["is_feature_info"]
    ]

    if (
        "field_attributes" in creation_options.__fields__
        and creation_options.field_attributes
    ):
        field_attributes = [
            field
            for field in field_attributes
            if field["name"] in creation_options.field_attributes
        ]

    return field_attributes
