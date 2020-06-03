from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field


class IndexType(str, Enum):
    gist = "gist"
    btree = "btree"
    hash = "hash"


class Index(BaseModel):
    index_type: IndexType
    column_name: str


class PartitionType(str, Enum):
    hash = "hash"
    list = "list"
    range = "range"


# class HashPartitionSchema(BaseModel):
#     partition_schema: int
#
#
# class ListPartitionSchema(BaseModel):
#     partition_schema: Dict[str, List[str]]
#
#
# class RangePartitionSchema(BaseModel):
#     partition_schema: Dict[str, Tuple[Any, Any]]


class Partitions(BaseModel):
    partition_type: PartitionType = Field(..., description="Partition type")
    partition_column: str = Field(
        ..., description="Column to be used to create partitions."
    )
    create_default: bool = Field(
        False,
        description="Create default partition to cache values not captured by partition schema",
    )
    partition_schema: Union[
        int, Dict[str, List[str]], Dict[str, Tuple[Any, Any]]
    ] = Field(
        ...,
        description="Partition Schema. "
        "For Hash Partition the number of columns (int)."
        "For List Partition a dictionaty where key=partition table suffix and value a list of values to use for each partition."
        "For Range Partition a dictionary where key=partition table suffix and value a tuple of start and end value for partition. End value is exclusive.",
    )


class VectorSourceCreationOptions(BaseModel):
    src_driver: str = Field(
        ..., description="Driver of source file. Must be an OGR driver"
    )
    zipped: bool = Field(..., description="Indicate if source file is zipped")
    layers: Optional[List[str]] = Field(
        None, description="List of input layers. Only required for .gdb and .gpkg"
    )
    indices: List[Index] = Field(
        [
            Index(index_type="gist", column_name="geom"),
            Index(index_type="gist", column_name="geom_wm"),
            Index(index_type="hash", column_name="gfw_geostore_id"),
        ],
        description="List of indices to add to table",
    )


class TableSourceCreationOptions(BaseModel):
    src_driver: str = Field(..., description="Driver of input file (CSV, TSV, ...)")
    delimiter: str = Field(..., description="Delimiter used in input file")
    has_header: bool = Field(True, description="Input file has header. Must be true")
    latitude: Optional[str] = Field(None, description="Column with latitude coordinate")
    longitude: Optional[str] = Field(
        None, description="Column with longitude coordinate"
    )
    cluster: Optional[Index] = Field(
        None, description="Index to use for clustering (optional)."
    )
    partitions: Optional[Partitions] = Field(
        None, description="Partitioning schema (optional)"
    )
    indices: List[Index] = Field([], description="List of indices to add to table")


CreationOptions = Union[VectorSourceCreationOptions, TableSourceCreationOptions]
