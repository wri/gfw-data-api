import logging
from typing import List

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import ORJSONResponse
from sqlalchemy.schema import CreateSchema, DropSchema

from . import dataset_dependency

from ..models.orm.dataset import Dataset as ORMDataset
from ..models.pydantic.dataset import Dataset, DatasetCreateIn, DatasetUpdateIn
from ..application import db
from ..settings.globals import USERNAME

router = APIRouter()


@router.get("/", response_class=ORJSONResponse, tags=["Dataset"], response_model=List[Dataset])
async def get_datasets():
    """
    Get list of all datasets
    """
    pass


@router.get("/{dataset}", response_class=ORJSONResponse, tags=["Dataset"], response_model=Dataset)
async def get_dataset(*, dataset: str = Depends(dataset_dependency)):
    """
    Get basic metadata and available versions for a given dataset
    """
    row: ORMDataset = await ORMDataset.get(dataset)
    return Dataset.from_orm(row)


@router.put("/{dataset}", response_class=ORJSONResponse, tags=["Dataset"], response_model=Dataset)
async def put_dataset(*, dataset: str = Depends(dataset_dependency), request: DatasetCreateIn):
    """
    Create or update a dataset
    """
    try:
        new_dataset: ORMDataset = await ORMDataset.create(dataset=dataset, **request.dict())
    except asyncpg.exceptions.UniqueViolationError:
        raise HTTPException(status_code=403, detail=f"Dataset with name {dataset} already exists")

    await db.status(CreateSchema(dataset))
    await db.status(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {dataset} GRANT SELECT ON TABLES TO {USERNAME};")

    return Dataset.from_orm(new_dataset)


@router.patch("/{dataset}", response_class=ORJSONResponse, tags=["Dataset"], response_model=Dataset)
async def patch_dataset(*, dataset: str = Depends(dataset_dependency), request: DatasetUpdateIn):
    """
    Partially update a dataset
    """
    logging.info(request)
    row: ORMDataset = await ORMDataset.get(dataset)

    updated_fields = request.dict(skip_defaults=True)

    # Make sure, existing metadata not mentioned in request remain untouched
    metadata = row.metadata
    metadata.update(updated_fields["metadata"])
    updated_fields["metadata"] = metadata

    # TODO make sure we validate data before updating database. Otherwise data are update but final check throws and error
    # updated_fields: Dataset = Dataset.from_orm(request)
    await row.update(**updated_fields).apply()
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
