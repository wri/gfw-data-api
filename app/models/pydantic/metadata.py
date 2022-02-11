from datetime import date, datetime
from typing import Any, Dict, List, Optional, Type, Union
from uuid import UUID

from pydantic import BaseModel, Field, StrictInt, validator
from pydantic.utils import GetterDict

from ..enum.assets import AssetType
from ..enum.pg_types import PGType
from .base import BaseRecord, StrictBaseModel
from .responses import Response


class FieldMetadata(StrictBaseModel):
    field_name_: str = Field(..., alias="field_name")
    field_alias: Optional[str]
    field_description: Optional[str]
    field_type: PGType
    is_feature_info: bool = True
    is_filter: bool = True

    class Config:
        orm_mode = True


class RasterFieldMetadata(StrictBaseModel):
    field_name_: str = Field(..., alias="field_name")
    field_alias: Optional[str]
    field_description: Optional[str]
    field_values: Optional[List[Any]]


class CommonMetadata(BaseModel):
    resolution: Optional[str]
    geographic_coverage: Optional[str]
    update_frequency: Optional[str]
    scale: Optional[str]

    class Config:
        schema_extra = {
            "examples": [
                {
                    "resolution": "10m x 10m",
                    "geographic_coverage": "Amazon Basin",
                    "update_frequency": "Updated daily, image revisit time every 5 days",
                    "scale": "regional",
                }
            ]
        }


class DatasetMetadata(CommonMetadata):
    title: str
    source: str
    license: str
    data_language: str
    overview: str

    function: Optional[str]
    citation: Optional[str]
    cautions: Optional[str]
    key_restrictions: Optional[str]
    keywords: Optional[List[str]]
    why_added: Optional[str]
    learn_more: Optional[str]

    class Config:
        schema_extra = {
            "examples": [
                {
                    "title": "Deforestation alerts (GLAD-S2)",
                    "source": "Global Land Analysis and Discovery (GLAD), University of Maryland",
                    "license": "[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)",
                    "data_language": "en",
                    "overview": "This data set is a forest loss alert product developed by the Global Land Analysis and Discovery lab at the University of Maryland. GLAD-S2 alerts utilize data from the European Space Agency's Sentinel-2 mission, which provides optical imagery at a 10m spatial resolution with a 5-day revisit time. The shorter revisit time, when compared to GLAD Landsat alerts, reduces the time to detect forest loss and between the initial detection of forest loss and classification as high confidence. This is particularly advantageous in wet and tropical regions, where persistent cloud cover may delay detections for weeks to months. GLAD-S2 alerts are available for primary forests in the Amazon basin from January 1st 2019 to present, updated daily. New Sentinel-2 images are analyzed as soon as they are acquired. Cloud, shadow, and water are filtered out of each new image, and a forest loss algorithm is applied to all remaining clear land observations. The algorithm relies on the spectral data in each new image in combination with spectral metrics from a baseline period of the previous two years. Alerts become high confidence when at least two of four subsequent observations are flagged as forest loss (this corresponds to 'high', 'medium', and 'low' confidence loss on the GLAD app linked below). The alert date represents the date of forest loss detection. Users can choose to display only high confidence alerts on the map, but keep in mind this will filter out the most recent detections of forest loss. Additionally, forest loss will not be detected again on pixels with high confidence alerts. Alerts that have not become high confidence within 180 days are removed from the data set.",
                    "function": "Identifies areas of primary forest loss  in near real time using Sentinel-2 imagery",
                    "citation": "Pickens, A.H., Hansen, M.C., Adusei, B., and Potapov P. 2020. Sentinel-2 Forest Loss Alert. Global Land Analysis and Discovery (GLAD), University of Maryland.",
                    "cautions": "Results are masked to only within the primary forest mask of [Turubanova et al (2018)](https://iopscience.iop.org/article/10.1088/1748-9326/aacd1c) in the Amazon river basin, with 2001-2018 forest loss from [Hansen et al. (2013)](https://science.sciencemag.org/content/342/6160/850) removed. Alerts that have been detected in two out of four consecutive images are classified as high confidence. Pixels with high confidence alerts cannot be alerted again. The accuracy of this product has not been assessed",
                    "keywords": ["Forest Change"],
                    "learn_more": "https://glad.earthengine.app/view/s2-forest-alerts",
                }
            ]
        }

    # other: Optional[str]


class DatasetMetadataOut(DatasetMetadata, BaseRecord):
    metadata_id: UUID


class DatasetMetadataIn(DatasetMetadata, StrictBaseModel):
    pass


class DatasetMetadataUpdate(DatasetMetadataIn):
    title: Optional[str]
    source: Optional[str]
    license: Optional[str]
    data_language: Optional[str]
    overview: Optional[str]


class ContentDateRange(StrictBaseModel):
    start_date: date = Field(
        ...,
        description="Beginning date covered by data",
    )
    end_date: date = Field(
        ...,
        description="End date covered by data",
    )

    @validator("start_date", "end_date", pre=True)
    def parse_date_str(cls, value):
        return _date_validator(value)


class VersionMetadataGetter(GetterDict):
    def get(self, key: str, default: Any = None) -> Any:
        if key == "content_date_range":
            return {
                "start_date": self._obj.content_start_date,
                "end_date": self._obj.content_end_date,
            }
        else:
            return super(VersionMetadataGetter, self).get(key, default)


class VersionMetadata(CommonMetadata):
    creation_date: date = Field(
        ...,
        description="Date resource was created",
    )
    content_date_range: Optional[ContentDateRange] = Field(
        ...,
        description="Date range covered by the content",
    )

    last_update: date = Field(
        ...,
        description="Date the data were last updated",
    )

    @validator("last_update", "creation_date", pre=True)
    def parse_date_str(cls, value):
        return _date_validator(value)

    class Config:
        schema_extra = {
            "examples": [
                {
                    "content_date_range": {
                        "start_date": "2000-01-01",  # TODO fix date
                        "end_date": "2021-04-06",
                    },
                    "creation_date": "2021-04-07",
                }
            ]
        }

    # added_date: Optional[str] = Field(
    #     None,
    #     description="Date the data were added to GFW",
    #     regex=DATE_REGEX,
    # )
    # download: Optional[str]
    # analysis: Optional[str]
    # data_updates: Optional[str]


class VersionMetadataIn(VersionMetadata, StrictBaseModel):
    pass


class VersionMetadataOut(VersionMetadata, BaseRecord):
    metadata_id: UUID

    class Config:
        getter_dict = VersionMetadataGetter


class VersionMetadataUpdate(VersionMetadataIn):
    creation_date: Optional[date] = Field(
        None,
        description="Date resource was created",
    )
    content_date_range: Optional[ContentDateRange] = Field(
        None,
        description="Date range covered by the content",
    )

    last_update: Optional[date] = Field(
        None,
        description="Date the data were last updated",
    )


class RasterTableRow(StrictBaseModel):
    value: int
    meaning: Any


class RasterTable(StrictBaseModel):
    rows: List[RasterTableRow]
    default_meaning: Optional[Any] = None


class RasterTileCacheMetadata(VersionMetadata):
    min_zoom: Optional[int]  # FIXME: Should this really be optional?
    max_zoom: Optional[
        int
    ]  # FIXME: Making required causes exception as it's never set. Find out why
    # TODO: More?


class RasterTileSetMetadata(VersionMetadata):
    # Raster Files/ Raster Tilesets
    raster_statistics: Optional[Dict[str, Any]]
    raster_table: Optional[RasterTable]
    raster_tiles: Optional[List[str]]
    data_type: Optional[str]
    compression: Optional[str]
    no_data_value: Optional[str]


class StaticVectorTileCacheMetadata(VersionMetadata):
    min_zoom: Optional[int]
    max_zoom: Optional[int]
    # fields_: Optional[List[FieldMetadata]] = Field(None, alias="fields")
    # TODO: default symbology/ legend


class DynamicVectorTileCacheMetadata(StaticVectorTileCacheMetadata):
    min_zoom: StrictInt = 0
    max_zoom: StrictInt = 22


class DatabaseTableMetadata(VersionMetadata):
    # fields_: Optional[List[FieldMetadata]] = Field(None, alias="fields")
    pass


class VectorFileMetadata(VersionMetadata):
    pass


AssetMetadata = Union[
    DatabaseTableMetadata,
    StaticVectorTileCacheMetadata,
    DynamicVectorTileCacheMetadata,
    RasterTileCacheMetadata,
    RasterTileSetMetadata,
    VectorFileMetadata,
]


class DatasetMetadataResponse(Response):
    data: DatasetMetadataOut


class VersionMetadataResponse(Response):
    data: VersionMetadataOut


class FieldMetadataResponse(Response):
    data: Union[List[FieldMetadata], List[RasterFieldMetadata]]


def asset_metadata_factory(asset_type: str, metadata: Dict[str, Any]) -> AssetMetadata:
    """Create Pydantic Asset Metadata class based on asset type."""
    metadata_factory: Dict[str, Type[AssetMetadata]] = {
        AssetType.static_vector_tile_cache: StaticVectorTileCacheMetadata,
        AssetType.dynamic_vector_tile_cache: DynamicVectorTileCacheMetadata,
        AssetType.raster_tile_cache: RasterTileCacheMetadata,
        AssetType.raster_tile_set: RasterTileSetMetadata,
        AssetType.database_table: DatabaseTableMetadata,
        AssetType.geo_database_table: DatabaseTableMetadata,
        AssetType.ndjson: VectorFileMetadata,
        AssetType.grid_1x1: VectorFileMetadata,
        AssetType.shapefile: VectorFileMetadata,
        AssetType.geopackage: VectorFileMetadata,
    }
    if asset_type in metadata_factory.keys():
        md: AssetMetadata = metadata_factory[asset_type](**metadata)

    else:
        raise NotImplementedError(
            f"Asset metadata factory for type {asset_type} not implemented"
        )

    return md


def _date_validator(date_str):
    if isinstance(date_str, date):
        return date_str
    return datetime.strptime(date_str, "%Y-%m-%d").date()
