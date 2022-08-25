"""Datasets are just a bucket, for datasets which share the same core
metadata."""
from typing import Optional, Union

from fastapi import APIRouter, Query, Request
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
async def get_datasets(
    request: Request,
    page_number: Optional[int] = Query(default=None, alias="page[number]", ge=1),
    page_size: Optional[int] = Query(default=None, alias="page[size]", ge=1),
) -> Union[PaginatedDatasetsResponse, DatasetsResponse]:
    """Get list of all datasets."""
    data, links, meta = await paginate_datasets(
        request_url=f"{request.url}".split("?")[0], page=page_number, size=page_size
    )

    if meta is None or links is None:
        return DatasetsResponse(data=data)

    return PaginatedDatasetsResponse(
        data=data, links=links._asdict(), meta=meta._asdict()
    )
