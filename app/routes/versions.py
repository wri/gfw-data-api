from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse

from app.routes import dataset_dependency, version_dependency


router = APIRouter()


@router.get("/{dataset}/{version}", response_class=ORJSONResponse, tags=["Dataset"])
async def get_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency)
):
    """
    Get basic metadata for a given version
    """
    pass


@router.put("/{dataset}/{version}", response_class=ORJSONResponse, tags=["Dataset"])
async def put_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency)
):
    """
    Create or update a version for a given dataset
    """
    pass


@router.patch("/{dataset}/{version}", response_class=ORJSONResponse, tags=["Dataset"])
async def patch_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency)
):
    """
    Partially update a version of a given dataset
    """
    pass


@router.delete("/{dataset}/{version}", response_class=ORJSONResponse, tags=["Dataset"])
async def delete_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency)
):
    """
    Delete a version
    """
    pass
