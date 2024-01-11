from typing import Any, Dict, List, Set

from ..crud import assets, metadata as metadata_crud
from ..models.orm.assets import Asset as ORMAsset
from ..models.pydantic.creation_options import CreationOptions


async def get_field_attributes(
    dataset: str, version: str, creation_options: CreationOptions
) -> List[Dict[str, Any]]:
    """Get list of field attributes on the asset which are marked as `is_feature_info`
    If a field list is provided in creation options, limit the list to those provided,
    in the order provided. Invalid provided fields are silently ignored.
    """

    default_asset: ORMAsset = await assets.get_default_asset(dataset, version)
    asset_fields = await metadata_crud.get_asset_fields_dicts(default_asset)

    name_to_feature_fields: Dict[str, Dict] = {
        field["name"]: field
        for field in asset_fields
        if field["is_feature_info"]
    }

    if (
        "field_attributes" in creation_options.__fields__
        and creation_options.field_attributes
    ):
        asset_field_attributes = [
            name_to_feature_fields[field_name]
            for field_name in creation_options.field_attributes
            if field_name in name_to_feature_fields
        ]
    else:
        asset_field_attributes = list(name_to_feature_fields.values())

    return asset_field_attributes
