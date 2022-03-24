from typing import Any, Dict, List, Optional, Type, Union
from uuid import UUID
from h11 import Data

from pydantic import BaseModel, Field, StrictInt, create_model

from ..enum.assets import AssetType
from ..enum.pg_types import PGType
from .base import BaseRecord, StrictBaseModel
from .responses import Response
from ...models.orm.asset_metadata import AssetMetadata as ORMAssetMetadata


class AssetBase(StrictBaseModel):
    name: str


class FieldMetadata(StrictBaseModel):
    name: str
    alias: Optional[str]
    description: Optional[str]
    data_type: PGType
    unit: Optional[str]
    is_feature_info: bool = True
    is_filter: bool = True


class FieldMetadataOut(FieldMetadata, BaseRecord):
    id: UUID


class RasterTableRow(StrictBaseModel):
    """
    Mapping of pixel value to what it represents in physical world.
    E.g., in ESA land cover data, 10 represents agriculture use.
    """
    value: int
    meaning: Any


class RasterTable(StrictBaseModel):
    rows: List[RasterTableRow]
    default_meaning: Optional[Any] = None


class RasterBandMetadata(StrictBaseModel):
    # Raster Files/ Raster Tilesets
    pixel_meaning: str
    unit: Optional[str]
    statistics: Optional[Dict[str, Any]]
    values_table: Optional[RasterTable]
    data_type: Optional[str]
    compression: Optional[str]
    no_data_value: Optional[str]


class RasterBandMetadataOut(RasterBandMetadata, BaseRecord):
    id: UUID


class RasterTileSetMetadata(AssetBase):
    bands: List[RasterBandMetadata]
    resolution: int


class RasterTileSetMetadataOut(RasterTileSetMetadata, BaseRecord):
    id: UUID
    bands: List[RasterBandMetadataOut]


class RasterTileCacheMetadata(AssetBase):
    min_zoom: Optional[int]  # FIXME: Should this really be optional?
    max_zoom: Optional[
        int
    ]  # FIXME: Making required causes exception as it's never set. Find out why
    # TODO: More?


class StaticVectorTileCacheMetadata(AssetBase):
    min_zoom: Optional[int]
    max_zoom: Optional[int]
    fields: Optional[List[FieldMetadata]] = Field(None, alias="fields")
    # TODO: default symbology/ legend


class DynamicVectorTileCacheMetadata(StaticVectorTileCacheMetadata):
    min_zoom: StrictInt = 0
    max_zoom: StrictInt = 22


class DatabaseTableMetadata(AssetBase):
    fields_: Optional[List[FieldMetadata]] = Field(None, alias="fields")


class VectorFileMetadata(AssetBase):
    fields_: Optional[List[FieldMetadata]] = Field(None, alias="fields")


AssetMetadata = Union[
    DatabaseTableMetadata,
    StaticVectorTileCacheMetadata,
    DynamicVectorTileCacheMetadata,
    RasterTileCacheMetadata,
    RasterTileSetMetadata,
    RasterBandMetadata,
    VectorFileMetadata
]


def asset_metadata_out(Metadata):
    if 'bands' in Metadata.__dict__['__fields__'].keys():
        return create_model(
            f"{Metadata.__name__}Out",
            __base__=(Metadata, BaseRecord),
            id=(UUID, ...),
            bands=(List[RasterBandMetadataOut], ...)
        )

    if 'fields' in Metadata.__dict__['__fields__']:
        return create_model(
            f"{Metadata.__name__}Out",
            __base__=(Metadata, BaseRecord),
            id=(UUID, ...),
            fields=(List[FieldMetadataOut], ...)
        )

    return create_model(
        f"{Metadata.__name__}Out",
        __base__=(Metadata, BaseRecord),
        id=(UUID, ...),
    )


AssetMetadataOutList = [
    asset_metadata_out(Metadata) for Metadata in AssetMetadata.__args__
]



# Instantiating Union doesn't support list or spread arguments so instantiating one
# with couple of the inputs and then setting its __args__ attr with all the parameters
AssetMetadataOut = Union[AssetMetadataOutList[0], AssetMetadataOutList[4]]
AssetMetadataOut.__setattr__('__args__', tuple(AssetMetadataOutList))


class AssetMetadataResponse(Response):
    data: AssetMetadataOut


def asset_metadata_factory(asset_type: str, metadata: ORMAssetMetadata) -> AssetMetadata:
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
        MetadataOut = asset_metadata_out(metadata_factory[asset_type])
        md: AssetMetadata = MetadataOut.from_orm(metadata)

    else:
        raise NotImplementedError(
            f"Asset metadata factory for type {asset_type} not implemented"
        )

    return md


class FieldMetadataResponse(Response):
    data: Union[List[FieldMetadata], List[FieldMetadata]]
