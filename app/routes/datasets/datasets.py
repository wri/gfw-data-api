"""Datasets are just a bucket, for datasets which share the same core
metadata."""
from typing import Any
from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse
from fastapi.logger import logger
from fastapi_pagination import Page, Params
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
async def get_datasets() -> DatasetsResponse:
    """Get list of all datasets."""
    data = await datasets.get_datasets()

    return DatasetsResponse(data=data)


@router.get(
    "/paginate",
    response_class=ORJSONResponse,
    tags=["Datasets"],
    response_model=Page[Any],
)
async def get_paginated_datasets(params: Params = Depends()) -> Any:
    logger.error("Will paginate!")
    logger.error(f"params: {params}")
    query = datasets.get_datasets_query()
    data = await paginate(query, params)
    return data
