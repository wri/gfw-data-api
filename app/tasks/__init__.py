from typing import Any, Dict, List, Tuple, Union

from ..application import ContextEngine, db
from ..crud import assets
from ..models.orm.queries.fields import fields
from ..models.pydantic.creation_options import Partitions
from ..models.pydantic.metadata import FieldMetadata
from ..settings.globals import (
    WRITER_DBNAME,
    WRITER_HOST,
    WRITER_PASSWORD,
    WRITER_PORT,
    WRITER_USERNAME,
)

writer_secrets = [
    {"name": "PGPASSWORD", "value": str(WRITER_PASSWORD)},
    {"name": "PGHOST", "value": WRITER_HOST},
    {"name": "PGPORT", "value": WRITER_PORT},
    {"name": "PGDATABASE", "value": WRITER_DBNAME},
    {"name": "PGUSER", "value": WRITER_USERNAME},
]


async def get_field_metadata(dataset: str, version: str) -> List[Dict[str, Any]]:
    """
    Get field list for asset and convert into Metadata object
    """

    rows = await db.all(fields, dataset=dataset, version=version)
    field_metadata = list()

    for row in rows:
        metadata = FieldMetadata.from_orm(row)
        if metadata.field_name_ in ["geom", "geom_wm", "gfw_geojson", "gfw_bbox"]:
            metadata.is_filter = False
            metadata.is_feature_info = False
        metadata.field_alias = metadata.field_name_
        field_metadata.append(metadata.dict())

    return field_metadata


async def update_asset_status(asset_id, status):
    """
    Update status of asset
    """
    async with ContextEngine("PUT"):
        await assets.update_asset(asset_id, status=status)


async def update_asset_field_metadata(dataset, version, asset_id):
    """
    Update asset field metadata
    """

    field_metadata: List[Dict[str, Any]] = await get_field_metadata(dataset, version)
    metadata = {"fields_": field_metadata}

    async with ContextEngine("PUT"):
        await assets.update_asset(asset_id, metadata=metadata)


def partition_parmas(
    dataset: str, version: str, partitions: Partitions
) -> List[Tuple[Union[str, int], str]]:

    params: List[Tuple[Union[str, int], str]] = list()
    if partitions.partition_type == "hash" and isinstance(
        partitions.partition_schema, int
    ):
        for i in range(partitions.partition_schema):
            sql = f'CREATE TABLE "{dataset}"."{version}_{i}" PARTITION OF "{dataset}"."{version}" FOR VALUES WITH (MODULUS {partitions.partition_schema}, REMAINDER {i})'
            params.append((i, sql))

    elif partitions.partition_type == "list" and isinstance(
        partitions.partition_schema, dict
    ):
        for key in partitions.partition_schema.keys():
            sql = f'CREATE TABLE "{dataset}"."{version}_{key}" PARTITION OF "{dataset}"."{version}" FOR VALUES IN {tuple(partitions.partition_schema[key])}'

            params.append((key, sql))
    elif partitions.partition_type == "range" and isinstance(
        partitions.partition_schema, dict
    ):
        for key in partitions.partition_schema.keys():
            sql = f"""CREATE TABLE "{dataset}"."{version}_{key}" PARTITION OF "{dataset}"."{version}" FOR VALUES FROM ('{partitions.partition_schema[key][0]}') TO ('{partitions.partition_schema[key][1]}')"""
            params.append((key, sql))
    else:
        NotImplementedError(
            "The Partition type and schema combination is not supported"
        )

    return params
