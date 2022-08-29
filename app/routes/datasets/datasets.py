"""Datasets are just a bucket, for datasets which share the same core
metadata."""
from typing import Optional, Union

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import ORJSONResponse

from app.utils.paginate import paginate_collection

from ...crud.datasets import count_datasets as count_datasets_fn
from ...crud.datasets import get_datasets as datasets_fn
from ...models.pydantic.datasets import DatasetsResponse, PaginatedDatasetsResponse

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
    """Get list of all datasets."""
    try:
        data, links, meta = await paginate_collection(
            paged_items_fn=datasets_fn,
            item_count_fn=count_datasets_fn,
            request_url=f"{request.url}".split("?")[0],
            page=page_number,
            size=page_size,
        )

        if meta is None or links is None:
            return DatasetsResponse(data=data)

        return PaginatedDatasetsResponse(
            data=data, links=links._asdict(), meta=meta._asdict()
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
