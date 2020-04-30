from typing import Type, Optional, Union, Dict, Any

from fastapi import Path, Query
from ..application import db
from pydantic import BaseModel

from ..models.orm.base import Base

VERSION_REGEX = r"^v\d{1,8}\.?\d{1,3}\.?\d{1,3}$|^latest$"


async def dataset_dependency(dataset: str = Path(..., title="Dataset")):
    return dataset


async def version_dependency(
    version: str = Path(..., title="Dataset version", regex=VERSION_REGEX)
):

    # if version == "latest":
    #     version = ...

    return version


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

    # new_row = model.from_orm(row)
    # new_row.metadata = metadata

    await row.update(**input_data).apply()

    return row
