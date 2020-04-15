from fastapi import APIRouter,  Depends
from fastapi.responses import ORJSONResponse

from app.routes import dataset_dependency


router = APIRouter()


@router.get("/", response_class=ORJSONResponse, tags=["Dataset"])
async def get_datasets():
    """
    Get list of all datasets
    """
    pass


@router.get("/{dataset}", response_class=ORJSONResponse, tags=["Dataset"])
async def get_dataset(*, dataset: str = Depends(dataset_dependency)):
    """
    Get basic metadata and available versions for a given dataset
    """
    pass


@router.put("/{dataset}", response_class=ORJSONResponse, tags=["Dataset"])
async def put_dataset(*, dataset: str = Depends(dataset_dependency)):
    """
    Create or update a dataset
    """
    pass


@router.patch("/{dataset}", response_class=ORJSONResponse, tags=["Dataset"])
async def patch_dataset(*, dataset: str = Depends(dataset_dependency)):
    """
    Partially update a dataset
    """
    pass


@router.delete("/{dataset}", response_class=ORJSONResponse, tags=["Dataset"])
async def delete_dataset(*, dataset: str = Depends(dataset_dependency)):
    """
    Delete a dataset
    """
    pass
