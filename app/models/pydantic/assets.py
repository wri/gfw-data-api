from typing import List, Optional
from uuid import UUID

from pydantic import Field

from ..enum.assets import AssetStatus, AssetType
from .base import BaseRecord, StrictBaseModel
from .creation_options import CreationOptions, OtherCreationOptions
from .metadata import AssetMetadata
from .responses import Response


class Asset(BaseRecord):
    asset_id: UUID
    dataset: str
    version: str
    asset_type: AssetType
    asset_uri: str
    status: AssetStatus = AssetStatus.pending
    is_managed: bool
    is_downloadable: bool
    metadata: AssetMetadata


class AssetCreateIn(StrictBaseModel):
    asset_type: AssetType
    asset_uri: Optional[str]
    is_managed: bool = True
    is_downloadable: Optional[bool] = Field(
        None,
        description="Flag to specify if assets associated with version can be downloaded."
        "If not set, value will default to settings of underlying version.",
    )
    creation_options: OtherCreationOptions
    metadata: Optional[AssetMetadata]


class AssetUpdateIn(StrictBaseModel):
    is_downloadable: Optional[bool] = Field(
        None,
        description="Flag to specify if assets associated with version can be downloaded."
        "If not set, value will default to settings of underlying version.",
    )
    metadata: Optional[AssetMetadata]


class AssetTaskCreate(StrictBaseModel):
    asset_type: AssetType
    dataset: str
    version: str
    asset_uri: Optional[str]
    is_managed: bool = True
    is_default: bool = False
    is_downloadable: Optional[bool] = None
    creation_options: CreationOptions  # should this also be OtherCreationOptions?
    metadata: Optional[AssetMetadata]


class AssetResponse(Response):
    data: Asset


class AssetsResponse(Response):
    data: List[Asset]

odd_examples = {
    "Add an auxiliary raster tile set using a gdal_calc expression": {
        "description": "Use the `calc` creation option to generate an auxiliary tile set by performing raster operations on the version's source raster tile set. Provide an expression in `numpy` (as `np`) syntax. Pixel meaning must be unique. Example below convertba",
        "value": {
            "asset_type": "Raster tile set",
            "creation_options": {
                "pixel_meaning": "Mg_CO2e_px",
                "data_type": "float32",
                "calc": "A * B / 10000",
                "grid": "10/40000",
                "no_data": "nan",
                "auxiliary_assets": [
                    "19172875-5961-4ba0-8ce5-18118268d937"
                ]
            }
        }
    },
    "Rasterize a vector asset": {
        "description": "Rasterize a version's source vector asset by providing PostgreSQL string as the `calc` creation option. Must provide a `raster_table` object in metadata.",
        "value": {
            "metadata": {
            "raster_table": {
                    "rows": [
                        {"value": 1, "meaning": "Caatinga"},
                        {"value": 2, "meaning": "Cerrado"},
                        {"value": 3, "meaning": "Pantanal"},
                        {"value": 4, "meaning": "Pampa"},
                        {"value": 5, "meaning": "Amazônia"},
                        {"value": 6, "meaning": "Mata Atlântica'"}
                    ]
                }
            },
            "asset_type": "Raster tile set",
            "creation_options": {
                "pixel_meaning": "name",
                "data_type": "uint8",
                "grid": "90/27008",
                "calc": "CASE WHEN name::text = 'Amazônia' THEN 5 WHEN name::text = 'Pantanal' THEN 3 WHEN name::text = 'Cerrado' THEN 2 WHEN name::text = 'Mata Atlântica' THEN 6 WHEN name::text = 'Pampa' THEN 4 WHEN name::text = 'Caatinga' THEN 1 ELSE 0 END"
            }
        }
    },
    "Add a raster tile cache with gradient symbology": {
        "description": "Data API will generate static PNG tiles up to `max_static_zoom` level and dynamically generate tiles up to `max_zoom`. `source_asset_id` must be an `asset_id` of any raster tile set within the same version. `symbology` object must be provided, with breakpoints as keys and RGB dicts as values.",
        "value": {
            "asset_type": "Raster tile cache",
            "creation_options": {
                "min_zoom": 0,
                "max_zoom": 12,
                "max_static_zoom": 9,
                "source_asset_id": "3124df3f-5816-4adf-b499-1456628e37ff",
                "symbology": {
                    "type": "gradient",
                    "colormap": {
                        "-99.99": {"red": 248, "green": 235, "blue": 255},
                        "-34.97": {"red": 241, "green": 215, "blue": 253},
                        "-23.35": {"red": 233, "green": 194, "blue": 253},
                        "-13.10": {"red": 226, "green": 172, "blue": 254},
                        "-13.05": {"red": 211, "green": 142, "blue": 255},
                        "-7.02": {"red": 194, "green": 109, "blue": 254},
                        "-6.75": {"red": 172, "green": 76, "blue": 250},
                        "-6.70": {"red": 147, "green": 53, "blue": 243},
                        "-6.65": {"red": 120, "green": 11, "blue": 229},
                        "1.42": {"red": 89, "green": 0, "blue": 203},
                        "2.45": {"red": 60, "green": 0, "blue": 171}
                    }
                }
            }
        }
    },
}