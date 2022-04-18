"""Datasets are just a bucket, for datasets which share the same core
metadata."""

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import ORJSONResponse
from sqlalchemy.schema import CreateSchema, DropSchema

from ...application import db
from ...authentication.token import is_admin
from ...crud import datasets, versions
from ...errors import RecordAlreadyExistsError, RecordNotFoundError
from ...models.orm.datasets import Dataset as ORMDataset
from ...models.orm.versions import Version as ORMVersion
from ...models.pydantic.datasets import (
    Dataset,
    DatasetCreateIn,
    DatasetResponse,
    DatasetUpdateIn,
)
from ...routes import dataset_dependency
from ...settings.globals import READER_USERNAME

router = APIRouter()


@router.get(
    "/{dataset}",
    response_class=ORJSONResponse,
    tags=["Datasets"],
    response_model=DatasetResponse,
)
async def get_dataset(*, dataset: str = Depends(dataset_dependency)) -> DatasetResponse:
    """Get basic metadata and available versions for a given dataset."""
    try:
        row: ORMDataset = await datasets.get_dataset(dataset)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return await _dataset_response(dataset, row)


@router.put(
    "/{dataset}",
    response_class=ORJSONResponse,
    tags=["Datasets"],
    response_model=DatasetResponse,
    status_code=201,
)
async def create_dataset(
    *,
    dataset: str = Depends(dataset_dependency),
    request: DatasetCreateIn,
    is_authorized: bool = Depends(is_admin),
    response: Response,
) -> DatasetResponse:
    """Create or update a dataset."""

    input_data: Dict = request.dict(exclude_none=True, by_alias=True)

    try:
        new_dataset: ORMDataset = await datasets.create_dataset(dataset, **input_data)
    except RecordAlreadyExistsError as e:
        raise HTTPException(status_code=400, detail=str(e))

    await db.status(CreateSchema(dataset))
    await db.status(f"GRANT USAGE ON SCHEMA {dataset} TO {READER_USERNAME};")
    await db.status(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {dataset} GRANT SELECT ON TABLES TO {READER_USERNAME};"
    )
    response.headers["Location"] = f"/{dataset}"

    return await _dataset_response(dataset, new_dataset)


@router.patch(
    "/{dataset}",
    response_class=ORJSONResponse,
    tags=["Datasets"],
    response_model=DatasetResponse,
)
async def update_dataset(
    *,
    dataset: str = Depends(dataset_dependency),
    request: DatasetUpdateIn,
    is_authorized: bool = Depends(is_admin),
) -> DatasetResponse:
    """Partially update a dataset.

    Only metadata field can be updated. All other fields will be
    ignored.
    """
    input_data: Dict = request.dict(exclude_none=True, by_alias=True)
    row: ORMDataset = await datasets.update_dataset(dataset, **input_data)

    return await _dataset_response(dataset, row)


@router.delete(
    "/{dataset}",
    response_class=ORJSONResponse,
    tags=["Datasets"],
    response_model=DatasetResponse,
)
async def delete_dataset(
    *,
    dataset: str = Depends(dataset_dependency),
    is_authorized: bool = Depends(is_admin),
) -> DatasetResponse:
    """Delete a dataset.

    By the time users are allowed to delete datasets, there should be no
    versions and assets left. So only thing beside deleting the dataset
    row is to drop the schema in the database.
    """

    version_rows: List[ORMVersion] = await versions.get_versions(dataset)
    if len(version_rows):
        raise HTTPException(
            status_code=409,
            detail="There are versions registered with the dataset."
            "Delete all related versions prior to deleting a dataset",
        )

    try:
        row: ORMDataset = await datasets.delete_dataset(dataset)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    # Delete all dataset related entries
    await db.status(DropSchema(dataset))

    return await _dataset_response(dataset, row)


async def _dataset_response(dataset: str, orm: ORMDataset) -> DatasetResponse:
    _versions: List[Any] = await versions.get_version_names(dataset)
    data: Dict = Dataset.from_orm(orm).dict(by_alias=True)
    data["versions"] = [version[0] for version in _versions]

    return DatasetResponse(data=Dataset(**data))
