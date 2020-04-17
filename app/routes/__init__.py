from typing import Type

from fastapi import Path
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


async def update_metadata(row: db.Model, request: BaseModel, model: Type[BaseModel]):
    """
    Merge updated metadata filed with existing fields
    """
    updated_fields = request.dict(skip_defaults=True)

    # Make sure, existing metadata not mentioned in request remain untouched
    if "metadata" in updated_fields.keys():
        metadata = row.metadata
        metadata.update(updated_fields["metadata"])
        updated_fields["metadata"] = metadata
        new_row = model.from_orm(row)
        new_row.metadata = metadata
        await row.update(**new_row.dict(skip_defaults=True)).apply()

    return row
