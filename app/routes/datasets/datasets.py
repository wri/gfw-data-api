"""Datasets are just a bucket, for datasets which share the same core
metadata."""
from typing import Union

from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse
from fastapi_pagination import Params
from fastapi_pagination.ext.gino import paginate

from ...crud import datasets
from ...models.pydantic.datasets import DatasetsResponse
from ...models.pydantic.responses import PaginatedResponse

router = APIRouter()


@router.get(
    "",
    response_class=ORJSONResponse,
    tags=["Datasets"],
    response_model=Union[DatasetsResponse, PaginatedResponse]
)
async def get_datasets(params: Params = Depends()) -> Union[DatasetsResponse, PaginatedResponse]:
    """Get list of all datasets."""
    if params.size < 50:
        query = datasets.get_datasets_query()
        data = await paginate(query, params)
        adapted = [dataset.__values__ for dataset in data.items]
        return PaginatedResponse(data=adapted,
                                    links={'self': "", 'first': "", 'last': "", 'prev': "", 'next': ""},
                                    meta={'size': data.size, 'total-pages': "", 'total-items': data.total})

    data = await datasets.get_datasets()
    return DatasetsResponse(data=data)
