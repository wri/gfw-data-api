"""Datasets are just a bucket, for datasets which share the same core
metadata."""

from fastapi import APIRouter
from fastapi.responses import ORJSONResponse

from ...models.pydantic.datasets import DatasetsResponse
from ...paginate.paginate import paginate_datasets

router = APIRouter()


@router.get(
    "",
    response_class=ORJSONResponse,
    tags=["Datasets"],
    response_model=DatasetsResponse,
)
async def get_datasets() -> DatasetsResponse:
    """Get list of all datasets."""
    data, _ = await paginate_datasets()

    return DatasetsResponse(data=data)
