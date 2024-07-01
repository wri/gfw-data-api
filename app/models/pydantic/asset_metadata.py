from typing import Any, Dict, List, Optional, Type, Union
from uuid import UUID

from fastapi.encoders import jsonable_encoder
from pydantic import StrictInt, create_model

from ...models.orm.assets import Asset as ORMAsset
from ..enum.assets import AssetType
from ..enum.pg_types import PGType
from .base import BaseORMRecord, StrictBaseModel
from .responses import Response


class AssetBase(StrictBaseModel):
    tags: Optional[str]


class FieldMetadata(StrictBaseModel):
    name: str
    alias: Optional[str]
    description: Optional[str]
    data_type: PGType
    unit: Optional[str]
    is_feature_info: bool = True
    is_filter: bool = True


class FieldMetadataOut(FieldMetadata):
    class Config:
        orm_mode = True


class FieldMetadataUpdate(StrictBaseModel):
    alias: Optional[str]
    description: Optional[str]
    unit: Optional[str]
    is_feature_info: Optional[bool]
    is_filter: Optional[bool]


class RasterTableRow(StrictBaseModel):
    """Mapping of pixel value to what it represents in physical world.

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
    description: Optional[str]
    statistics: Optional[Dict[str, Any]]
    values_table: Optional[RasterTable]
    data_type: Optional[str]
    compression: Optional[str]
    no_data_value: Optional[str]


class RasterBandMetadataOut(RasterBandMetadata):
    class Config:
        orm_mode = True


class RasterTileSetMetadata(AssetBase):
    bands: List[RasterBandMetadata]
    resolution: Optional[int]


class RasterTileSetMetadataUpdate(AssetBase):
    resolution: int


class RasterTileSetMetadataOut(RasterTileSetMetadata, BaseORMRecord):
    id: UUID
    bands: List[RasterBandMetadata]


class RasterTileCacheMetadata(AssetBase):
    min_zoom: Optional[int]  # FIXME: Should this really be optional?
    max_zoom: Optional[
        int
    ]  # FIXME: Making required causes exception as it's never set. Find out why
    # TODO: More?
    fields: Optional[List[FieldMetadata]]


class StaticVectorTileCacheMetadata(AssetBase):
    min_zoom: Optional[int]
    max_zoom: Optional[int]
    fields: Optional[List[FieldMetadata]]
    # TODO: default symbology/ legend


class StaticVectorTileCacheMetadataUpdate(AssetBase):
    min_zoom: Optional[int]
    max_zoom: Optional[int]


class DynamicVectorTileCacheMetadata(StaticVectorTileCacheMetadata):
    min_zoom: StrictInt = 0
    max_zoom: StrictInt = 22


class DatabaseTableMetadata(AssetBase):
    fields: Optional[List[FieldMetadata]]


class VectorFileMetadata(AssetBase):
    fields: Optional[List[FieldMetadata]]


AssetMetadata = Union[
    DatabaseTableMetadata,
    StaticVectorTileCacheMetadata,
    DynamicVectorTileCacheMetadata,
    RasterTileCacheMetadata,
    RasterTileSetMetadata,
    VectorFileMetadata,
]

AssetMetadataUpdate = Union[
    DynamicVectorTileCacheMetadata,
    RasterTileSetMetadataUpdate,
    StaticVectorTileCacheMetadataUpdate,
]


def asset_metadata_out(Metadata):
    if "bands" in Metadata.__dict__["__fields__"].keys():
        return create_model(
            f"{Metadata.__name__}Out",
            __base__=(Metadata, BaseORMRecord),
            id=(UUID, ...),
            bands=(Optional[List[RasterBandMetadataOut]], None),
        )

    if "fields" in Metadata.__dict__["__fields__"]:
        return create_model(
            f"{Metadata.__name__}Out",
            __base__=(Metadata, BaseORMRecord),
            id=(UUID, ...),
            fields=(Optional[List[FieldMetadataOut]], None),
        )

    return create_model(
        f"{Metadata.__name__}Out",
        __base__=(Metadata, BaseORMRecord),
        id=(UUID, ...),
    )


AssetMetadataOutList = [
    asset_metadata_out(Metadata) for Metadata in AssetMetadata.__args__
]


# Instantiating Union doesn't support list or spread arguments so instantiating one
# with couple of the inputs and then setting its __args__ attr with all the parameters
AssetMetadataOut = Union[AssetMetadataOutList[0], AssetMetadataOutList[1]]
AssetMetadataOut.__setattr__("__args__", tuple(AssetMetadataOutList))


class AssetMetadataResponse(Response):
    data: AssetMetadataOut


def asset_metadata_factory(asset: ORMAsset) -> AssetMetadata:
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

    if asset.asset_type in metadata_factory.keys():
        MetadataOut = asset_metadata_out(metadata_factory[asset.asset_type])
        metadata = getattr(asset, "metadata", None)
        if metadata:
            fields = getattr(metadata, "fields", None)
            bands = getattr(metadata, "bands", None)
            metadata_dict = jsonable_encoder(
                metadata.__dict__["__values__"], exclude=["asset_id"], exclude_none=True
            )
            if fields:
                fields_list = [
                    jsonable_encoder(
                        field.__dict__["__values__"],
                        exclude=["asset_metadata_id"],
                        exclude_none=True,
                    )
                    for field in fields
                ]
                metadata_dict["fields"] = fields_list
            if bands:
                bands_list = [
                    jsonable_encoder(
                        band.__dict__["__values__"],
                        exclude=["asset_metadata_id"],
                        exclude_none=True,
                    )
                    for band in bands
                ]
                metadata_dict["bands"] = bands_list
            md: AssetMetadata = MetadataOut(**metadata_dict)
        else:
            md = BaseORMRecord()
    else:
        raise NotImplementedError(
            f"Asset metadata factory for type {asset.asset_type} not implemented"
        )

    return md


class FieldsMetadataResponse(Response):
    data: List[FieldMetadataOut]


class FieldMetadataResponse(Response):
    data: FieldMetadataOut

class RasterBandsMetadataResponse(Response):
    data: List[RasterBandMetadata]
