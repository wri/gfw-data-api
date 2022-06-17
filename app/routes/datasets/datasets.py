"""Datasets are just a bucket, for datasets which share the same core
metadata."""
from typing import Union

from fastapi import APIRouter
from fastapi.responses import ORJSONResponse

from ...models.pydantic.datasets import DatasetsResponse, PaginatedDatasetsResponse
from ...paginate.paginate import paginate_datasets

router = APIRouter()


@router.get(
    "",
    response_class=ORJSONResponse,
    tags=["Datasets"],
    response_model=Union[PaginatedDatasetsResponse, DatasetsResponse],
)
async def get_datasets() -> Union[PaginatedDatasetsResponse, DatasetsResponse]:
    """Get list of all datasets."""
    data, meta = await paginate_datasets()

    if meta is None:
        return DatasetsResponse(data=data)
    return PaginatedDatasetsResponse(data=data, meta=meta.__dict__)
