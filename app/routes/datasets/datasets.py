"""Datasets are just a bucket, for datasets which share the same core
metadata."""
from typing import Optional, Union

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import ORJSONResponse

from app.crud.datasets import count_datasets as count_datasets_fn
from app.crud.datasets import get_datasets as datasets_fn
from app.models.pydantic.datasets import DatasetsResponse, PaginatedDatasetsResponse
from app.settings.globals import API_URL
from app.utils.paginate import paginate_collection

router = APIRouter()


@router.get(
    "",
    response_class=ORJSONResponse,
    tags=["Datasets"],
    response_model=Union[PaginatedDatasetsResponse, DatasetsResponse],
)
async def get_datasets(
    request: Request,
    page_number: Optional[int] = Query(
        default=None, alias="page[number]", ge=1, description="The page number."
    ),
    page_size: Optional[int] = Query(
        default=None,
        alias="page[size]",
        ge=1,
        description="The number of datasets per page. Default is `10`.",
    ),
) -> Union[PaginatedDatasetsResponse, DatasetsResponse]:
    """Get list of all datasets.

    Will attempt to paginate if `page[size]` or `page[number]` is
    provided. Otherwise, it will attempt to return the entire list of
    datasets in the response.
    """
    if page_number or page_size:
        try:
            data, links, meta = await paginate_collection(
                paged_items_fn=datasets_fn,
                item_count_fn=count_datasets_fn,
                request_url=f"{API_URL}{request.url.path}",
                page=page_number,
                size=page_size,
            )

            return PaginatedDatasetsResponse(data=data, links=links, meta=meta)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    all_datasets = await datasets_fn()
    return DatasetsResponse(data=all_datasets)
