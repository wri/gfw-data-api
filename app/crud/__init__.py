from typing import Union, Dict, Any

from pydantic.main import BaseModel

from app.application import db


async def update_data(
    row: db.Model, input_data: Union[BaseModel, Dict[str, Any]]  # type: ignore
) -> db.Model:  # type: ignore
    """
    Merge updated metadata filed with existing fields
    """
    if isinstance(input_data, BaseModel):
        input_data = input_data.dict(skip_defaults=True)

    # Make sure, existing metadata not mentioned in request remain untouched
    if "metadata" in input_data.keys():
        metadata = row.metadata
        metadata.update(input_data["metadata"])
        input_data["metadata"] = metadata

    if "change_log" in input_data.keys():
        change_log = row.change_log
        change_log.extend(input_data["change_log"])
        input_data["change_log"] = change_log

    await row.update(**input_data).apply()

    return row
