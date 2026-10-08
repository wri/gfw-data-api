from typing import Any, Dict, List, Optional

from ..crud.assets import get_default_asset
from ..crud.metadata import get_asset_fields_dicts
from ..models.orm.assets import Asset as ORMAsset
from ..models.pydantic.creation_options import CreationOptions


async def get_field_attributes(
    dataset: str, version: str, creation_options: CreationOptions
) -> List[Dict[str, Any]]:
    """Get list of field attributes on the asset which are marked as `is_feature_info`
    If a field list is provided in creation options, limit the list to those provided,
    in the order provided. Invalid provided fields are silently ignored.
    """

    default_asset: ORMAsset = await get_default_asset(dataset, version)
    asset_fields = await get_asset_fields_dicts(default_asset)

    name_to_feature_fields: Dict[str, Dict] = {
        field["name"]: field
        for field in asset_fields
        if field["is_feature_info"]
    }

    # Only the static vector creation options (which specify field_attributes as a
    # list of field names) are passed to this function.
    field_names: Optional[List[str]] = getattr(
        creation_options, "field_attributes", None
    )
    if field_names:
        asset_field_attributes = [
            name_to_feature_fields[field_name]
            for field_name in field_names
            if field_name in name_to_feature_fields
        ]
    else:
        asset_field_attributes = list(name_to_feature_fields.values())

    return asset_field_attributes
