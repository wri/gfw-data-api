from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse

from app.routes import dataset_dependency, version_dependency

router = APIRouter()


@router.get(
    "/{dataset}/{version}/fields", response_class=ORJSONResponse, tags=["Features"],
)
async def get_fields(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency)
):
    """
    Get fields (attribute names and types) for a given dataset version
    """
    pass
