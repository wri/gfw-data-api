import json
from copy import deepcopy
from typing import Any, Dict, List, Union

from pydantic.main import BaseModel

from ..application import db
from ..models.orm.base import Base
from ..models.pydantic.change_log import ChangeLog


async def update_data(
    row: db.Model, input_data: Union[BaseModel, Dict[str, Any]]  # type: ignore
) -> db.Model:  # type: ignore
    """Merge updated metadata filed with existing fields."""

    if isinstance(input_data, BaseModel):
        input_data = input_data.dict(skip_defaults=True)

    # Make sure, existing metadata not mentioned in request remain untouched
    if "metadata" in input_data.keys() and input_data["metadata"]:
        metadata = row.metadata
        metadata.update(input_data["metadata"])
        input_data["metadata"] = metadata

    if "change_log" in input_data.keys() and input_data["change_log"]:
        change_log = row.change_log
        # Make sure dates are correctly parsed as strings
        _logs = list()
        for data in input_data["change_log"]:
            _log = ChangeLog(**data).json()
            _logs.append(json.loads(_log))

        change_log.extend(_logs)
        input_data["change_log"] = change_log

    await row.update(**input_data).apply()

    return row


def update_metadata(row: Base, parent: Base):
    """Dynamically update metadata with parent metadata.

    Make sure empty metadata get correctly merged.
    """

    if parent.metadata:
        _metadata = deepcopy(parent.metadata)
    else:
        _metadata = {}

    if row.metadata:
        filtered_metadata = {
            key: value for key, value in row.metadata.items() if value is not None
        }
    else:
        filtered_metadata = {}

    _metadata.update(filtered_metadata)
    row.metadata = _metadata
    return row


def update_all_metadata(rows: List[Base], parent: Base) -> List[Base]:
    """Updates metadata for a list of records."""
    new_rows = list()
    for row in rows:
        update_metadata(row, parent)
        new_rows.append(row)

    return new_rows
