import logging
from typing import List

from asyncpg.exceptions import UniqueViolationError
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import ORJSONResponse
from sqlalchemy.schema import CreateSchema, DropSchema


from . import dataset_dependency, update_metadata
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
    sql = """
    select datasets.created_on, datasets.updated_on, datasets.dataset, datasets.metadata , version_array as versions
from
datasets left join (
select dataset, json_agg(row_to_json(versions)) as version_array
from versions group by dataset) t using (dataset);"""
    rows = await db.status(db.text(sql))
    # rows: List[ORMDataset] = await ORMDataset.query.gino.all()
    return rows[1]


@router.get("/{dataset}", response_class=ORJSONResponse, tags=["Dataset"], response_model=Dataset)
async def get_dataset(*, dataset: str = Depends(dataset_dependency)):
    """
    Get basic metadata and available versions for a given dataset
    """
    row: ORMDataset = await ORMDataset.get(dataset)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Dataset with name {dataset} does not exist")

    versions: List[ORMVersion] = await ORMVersion.query.where(ORMVersion.dataset == dataset).gino.all()
    # logging.debug(f"VERSIONS: {versions}")
    response = Dataset.from_orm(row).dict(by_alias=True)
    response["versions"] = versions
    return response


@router.put("/{dataset}/metadata", response_class=ORJSONResponse, tags=["Dataset"], response_model=Dataset)
async def put_dataset(*, dataset: str = Depends(dataset_dependency), request: DatasetCreateIn):
    """
    Create or update a dataset
    """
    try:
        new_dataset: ORMDataset = await ORMDataset.create(dataset=dataset, **request.dict())
    except UniqueViolationError:
        raise HTTPException(status_code=400, detail=f"Dataset with name {dataset} already exists")

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

    row = await update_metadata(row, request, Dataset)

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
