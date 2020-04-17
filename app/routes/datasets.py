import logging
from typing import List

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import ORJSONResponse
from sqlalchemy.schema import CreateSchema, DropSchema


from . import dataset_dependency

from ..models.orm.dataset import Dataset as ORMDataset
from ..models.orm.version import Version as ORMVersion
from ..models.pydantic.dataset import Dataset, DatasetCreateIn, DatasetUpdateIn
from ..application import db
from ..settings.globals import READER_USERNAME

router = APIRouter()


@router.get("/", response_class=ORJSONResponse, tags=["Dataset"], response_model=List[Dataset])
async def get_datasets():
    """
    Get list of all datasets
    """
    rows: List[ORMDataset] = await ORMDataset.query.gino.all()
    return rows


@router.get("/{dataset}", response_class=ORJSONResponse, tags=["Dataset"]) #, response_model=Dataset)
async def get_dataset(*, dataset: str = Depends(dataset_dependency)):
    """
    Get basic metadata and available versions for a given dataset
    """
    row: ORMDataset = await ORMDataset.get(dataset)
    versions: List[ORMVersion] = await ORMVersion.query.where("dataset" == dataset).gino.all()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Dataset with name {dataset} does not exist")
    response = Dataset.from_orm(row).dict()
    response["versions"] = versions
    return response


@router.put("/{dataset}/metadata", response_class=ORJSONResponse, tags=["Dataset"], response_model=Dataset)
async def put_dataset(*, dataset: str = Depends(dataset_dependency), request: DatasetCreateIn):
    """
    Create or update a dataset
    """
    try:
        new_dataset: ORMDataset = await ORMDataset.create(dataset=dataset, **request.dict())
    except asyncpg.exceptions.UniqueViolationError:
        raise HTTPException(status_code=403, detail=f"Dataset with name {dataset} already exists")

    await db.status(CreateSchema(dataset))
    await db.status(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {dataset} GRANT SELECT ON TABLES TO {READER_USERNAME};")

    return Dataset.from_orm(new_dataset)


@router.patch("/{dataset}", response_class=ORJSONResponse, tags=["Dataset"], response_model=Dataset)
async def patch_dataset(*, dataset: str = Depends(dataset_dependency), request: DatasetUpdateIn):
    """
    Partially update a dataset. Only metadata field can be updated. All other fields will be ignored.
    """

    row: ORMDataset = await ORMDataset.get(dataset)

    if row is None:
        raise HTTPException(status_code=404, detail=f"Dataset with name {dataset} does not exists")

    updated_fields = request.dict(skip_defaults=True)

    # Make sure, existing metadata not mentioned in request remain untouched
    if "metadata" in updated_fields.keys():
        metadata = row.metadata
        metadata.update(updated_fields["metadata"])
        updated_fields["metadata"] = metadata
        logging.info("write")
        new_row: Dataset = Dataset.from_orm(row)
        new_row.metadata = metadata
        logging.info(f"NEW ROW: {new_row}")

        await row.update(**new_row.dict(skip_defaults=True)).apply()
    return Dataset.from_orm(row)


@router.delete("/{dataset}", response_class=ORJSONResponse, tags=["Dataset"])
async def delete_dataset(*, dataset: str = Depends(dataset_dependency)):
    """
    Delete a dataset
    """

    row: ORMDataset = await ORMDataset.get(dataset)
    await ORMDataset.delete.where(ORMDataset.dataset == dataset).gino.status()
    await db.status(DropSchema(dataset))

    return Dataset.from_orm(row)
