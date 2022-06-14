"""Datasets are just a bucket, for datasets which share the same core
metadata."""
from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse
from fastapi_pagination import Params
from fastapi_pagination.ext.gino import paginate

from ...crud import datasets
from ...models.pydantic.datasets import DatasetsResponse

router = APIRouter()


@router.get(
    "",
    response_class=ORJSONResponse,
    tags=["Datasets"],
    response_model=DatasetsResponse,
)
async def get_datasets(params: Params = Depends()) -> DatasetsResponse:
    """Get list of all datasets."""
    if params.size < 50:
        query = datasets.get_datasets_query()
        data = await paginate(query, params)
        adapted = [dataset.__values__ for dataset in data.items]
        return DatasetsResponse(data=adapted,
                                    links={'self': "", 'first': "", 'last': "", 'prev': "", 'next': ""},
                                    meta={'size': data.size, 'total-pages': "", 'total-items': data.total},
                                    status='success')

    data = await datasets.get_datasets()
    return DatasetsResponse(data=data, status='success')

