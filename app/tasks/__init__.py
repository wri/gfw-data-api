from typing import Any, Dict, List

from ..application import db
from ..models.orm.queries.fields import fields
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
    rows = await db.gino.status(fields, dataset=dataset, version=version)
    field_metadata = list()
    for row in rows[1]:
        metadata = FieldMetadata.from_orm(row)
        if metadata.field_name_ in ["geom", "geom_wm", "gfw_geojson", "gfw_bbox"]:
            metadata.is_filter = False
            metadata.is_feature_info = False
        field_metadata.append(metadata)
    return field_metadata
