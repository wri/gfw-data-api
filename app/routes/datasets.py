from typing import Any, Dict, List

from fastapi import APIRouter, Depends, Response
from fastapi.responses import ORJSONResponse
from sqlalchemy.schema import CreateSchema, DropSchema

from ..application import db
from ..crud import datasets, versions
from ..models.orm.datasets import Dataset as ORMDataset
from ..models.pydantic.datasets import Dataset, DatasetCreateIn, DatasetUpdateIn
from ..routes import is_admin
from ..settings.globals import READER_USERNAME
from . import dataset_dependency

router = APIRouter()


# TODO:
#  - set default asset type for a dataset (can be overriden by versions)


@router.get(
    "/", response_class=ORJSONResponse, tags=["Dataset"], response_model=List[Dataset]
)
async def get_datasets():
    """
    Get list of all datasets
    """
    return await datasets.get_datasets()


@router.get(
    "/{dataset}",
    response_class=ORJSONResponse,
    tags=["Dataset"],
    response_model=Dataset,
)
async def get_dataset(*, dataset: str = Depends(dataset_dependency)):
    """
    Get basic metadata and available versions for a given dataset
    """
    row: ORMDataset = await datasets.get_dataset(dataset)
    return await _dataset_response(dataset, row)


@router.put(
    "/{dataset}",
    response_class=ORJSONResponse,
    tags=["Dataset"],
    response_model=Dataset,
    status_code=201,
)
async def create_dataset(
    *,
    dataset: str = Depends(dataset_dependency),
    request: DatasetCreateIn,
    is_authorized: bool = Depends(is_admin),
    response: Response,
):
    """
    Create or update a dataset
    """
    new_dataset: ORMDataset = await datasets.create_dataset(dataset, **request.dict())

    await db.status(CreateSchema(dataset))
    await db.status(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {dataset} GRANT SELECT ON TABLES TO {READER_USERNAME};"
    )
    response.headers["Location"] = f"/{dataset}"
    return await _dataset_response(dataset, new_dataset)


@router.patch(
    "/{dataset}",
    response_class=ORJSONResponse,
    tags=["Dataset"],
    response_model=Dataset,
)
async def update_dataset_metadata(
    *,
    dataset: str = Depends(dataset_dependency),
    request: DatasetUpdateIn,
    is_authorized: bool = Depends(is_admin),
):
    """
    Partially update a dataset. Only metadata field can be updated. All other fields will be ignored.
    """

    row: ORMDataset = await datasets.update_dataset(dataset, request)

    return await _dataset_response(dataset, row)


@router.delete(
    "/{dataset}",
    response_class=ORJSONResponse,
    tags=["Dataset"],
    response_model=Dataset,
)
async def delete_dataset(
    *,
    dataset: str = Depends(dataset_dependency),
    is_authorized: bool = Depends(is_admin),
):
    """
    Delete a dataset
    """

    row: ORMDataset = await datasets.delete_dataset(dataset)
    await db.status(DropSchema(dataset))

    return await _dataset_response(dataset, row)


async def _dataset_response(dataset: str, orm: ORMDataset) -> Dict[str, Any]:

    _versions: List[Any] = await versions.get_version_names(dataset)
    response = Dataset.from_orm(orm).dict(by_alias=True)
    response["versions"] = [version[0] for version in _versions]

    return response
