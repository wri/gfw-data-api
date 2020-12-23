from datetime import date
from typing import Any, Dict, List, Optional, Type, Union

from pydantic import Field
from pydantic.main import BaseModel

from ..enum.assets import AssetType
from ..enum.pg_types import (
    PGDateType,
    PGGeometryType,
    PGNumericType,
    PGTextType,
    PGType,
)
from ..pydantic.geostore import FeatureCollection
from ..pydantic.responses import Response


class FieldStats(BaseModel):
    field_name: str = Field(..., description="Field name")
    field_type: PGType = Field(..., description="Field data type (PostgreSQL)")


class NumericFieldStats(FieldStats):
    field_type: PGNumericType
    min: float = Field(..., description="Minimum value in column.")
    max: float = Field(..., description="Maximum value in column.")
    sum: float = Field(..., description="Sum of column.")
    mean: float = Field(..., description="Mean value of column.")
    std_dev: float = Field(..., description="Standard deviation of column.")


class TextFieldStats(FieldStats):
    field_type: PGTextType
    discrete_values: Optional[List[str]] = Field(
        None,
        description="A text field is considered to contain discrete values if it carries no more than 20 distinct values.",
    )


class DateFieldStats(FieldStats):
    field_type: PGDateType
    min: date = Field(..., description="Minimum value in column.")
    max: date = Field(..., description="Maximum value in column.")


class GeometryFieldStats(FieldStats):
    field_type: PGGeometryType
    geometry_types: List[str] = Field(
        ..., description="Geometry types contained in dataset"
    )
    crs: str = Field(..., description="Coordinate reference system.")
    extent: FeatureCollection = Field(
        ..., description="GeoJSON representation of feature extent."
    )


class TableStats(BaseModel):
    row_count: int = Field(..., description="Total row count.")
    field_stats: List[
        Union[NumericFieldStats, TextFieldStats, DateFieldStats, GeometryFieldStats]
    ] = Field(..., description="Statistics for selected field types.")


class Histogram(BaseModel):
    bin_count: int = Field(..., description="Number of bins in histogram.")
    min: float = Field(..., description="Minimum bin value.")
    max: float = Field(..., description="Maximum bin value.")
    value_count: List[int] = Field(..., description="Value count for each bin.")

    class Config:
        extra = "forbid"


class Affine(BaseModel):
    a: float = Field(..., description="Scale factor x")
    b: float = Field(..., description="Shear angle x")
    c: float = Field(..., description="Offset x")
    d: float = Field(..., description="Shear angle y")
    e: float = Field(..., description="Scale factor y")
    f: float = Field(..., description="Offset y")

    class Config:
        extra = "forbid"


class BandStats(BaseModel):
    # origin: Tuple[float, float] = Field(..., description="Raster origin.")
    min: float = Field(..., description="Minimum pixel value.")
    max: float = Field(..., description="Maximum pixel value.")
    mean: float = Field(..., description="Mean pixel value.")

    histogram: Optional[Histogram] = Field(description="Histogram.")

    class Config:
        extra = "forbid"


class RasterStats(BaseModel):
    # driver: str = Field(..., description="Driver used to create raster file.")
    # interleave: str = Field(..., description="Interleave strategy.")
    # tiled: bool = Field(..., description="Raster file is tiled or not.")
    # blockxsize: int = Field(..., description="Width of tiles or strips")
    # blockysize: int = Field(..., description="Height of tiles or stripes.")
    # compress: str = Field(..., description="Image compression used.")
    # nodata: Optional[int] = Field(..., description="No data value.")
    # dtype: str = Field(..., description="Pixel data type.")
    # width: int = Field(..., description="Raster width.")
    # height: int = Field(..., description="Raster height.")
    # bounds: Tuple[float, float, float, float] = Field(..., description="Raster bounds.")
    # transform: Affine = Field(..., description="Affine transformation.")
    # crs: str = Field(..., description="Coordinate reference system.")
    # pixel_size: Tuple[float, float] = Field(..., description="Raster pixel size.")
    bands: List[BandStats]

    class Config:
        extra = "forbid"


Stats = Union[TableStats, RasterStats]


class StatsResponse(Response):
    data: Optional[Stats]


def stats_factory(asset_type: str, input_data: Dict[str, Any]) -> Optional[Stats]:
    stats_constructor: Dict[str, Type[Stats]] = {
        AssetType.database_table: TableStats,
        AssetType.geo_database_table: TableStats,
        AssetType.dynamic_vector_tile_cache: TableStats,
        AssetType.static_vector_tile_cache: TableStats,
        AssetType.shapefile: TableStats,
        AssetType.ndjson: TableStats,
        AssetType.geopackage: TableStats,
        AssetType.raster_tile_set: RasterStats,
        AssetType.grid_1x1: TableStats,
    }

    if not input_data:
        return None

    try:
        return stats_constructor[asset_type](**input_data)
    except KeyError:
        raise NotImplementedError(f"Stats for type {asset_type} not implemented")
