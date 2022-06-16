"""Datasets are just a bucket, for datasets which share the same core
metadata."""

from fastapi import APIRouter
from fastapi.responses import ORJSONResponse

from ...paginate.paginate import paginate_datasets
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
    data = await paginate_datasets()

    return DatasetsResponse(data=data)
