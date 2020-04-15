from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse

from app.routes import dataset_dependency, version_dependency

router = APIRouter()


@router.get(
    "/{dataset}/{version}/sources", response_class=ORJSONResponse, tags=["Sources"]
)
async def get_sources(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency)
):
    """
    List all external source files used to seed dataset version
    """
    pass


@router.post(
    "/{dataset}/{version}/sources", response_class=ORJSONResponse, tags=["Sources"]
)
async def post_sources(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency)
):
    """
    Add (appends) a new source to the dataset version
    """
    pass


@router.patch(
    "/{dataset}/{version}/sources", response_class=ORJSONResponse, tags=["Sources"]
)
async def patch_sources(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency)
):
    """
    Overwrites existing data with data from new source
    """
    pass


@router.delete(
    "/{dataset}/{version}/sources", response_class=ORJSONResponse, tags=["Sources"]
)
async def delete_sources(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency)
):
    """
    Deletes existing data
    """
    pass
