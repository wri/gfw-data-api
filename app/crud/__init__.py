import json
from typing import Any, Dict, Union

from pydantic.main import BaseModel

from ..application import db
from ..models.pydantic.change_log import ChangeLog


async def update_data(
    row: db.Model, input_data: Union[BaseModel, Dict[str, Any]]  # type: ignore
) -> db.Model:  # type: ignore
    """Merge updated metadata filed with existing fields."""

    if not input_data:
        return row

    if isinstance(input_data, BaseModel):
        input_data = input_data.dict(skip_defaults=True, by_alias=True)

    if input_data.get("change_log"):
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
