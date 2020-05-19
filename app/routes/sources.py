from typing import List, Optional

from fastapi import APIRouter, Depends, File, UploadFile
from fastapi.responses import ORJSONResponse

from ..models.pydantic.source import Source
from ..routes import dataset_dependency, is_admin, version_dependency

router = APIRouter()


@router.get(
    "/{dataset}/{version}/sources",
    response_class=ORJSONResponse,
    tags=["Sources"],
    response_model=Source,
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
    "/{dataset}/{version}/sources",
    response_class=ORJSONResponse,
    tags=["Sources"],
    response_model=Source,
)
async def add_new_sources(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    files: Optional[List[UploadFile]] = File(None),
    is_authorized: bool = Depends(is_admin)
):
    """
    Add (appends) a new source to the dataset version
    """
    # TODO:
    #  Verify source type
    #  Copy files to data lake (raw subfolder)
    #  Update all existing assets (append/ partially update if possible, otherwise recreate)

    pass


@router.patch(
    "/{dataset}/{version}/sources",
    response_class=ORJSONResponse,
    tags=["Sources"],
    response_model=Source,
)
async def update_sources(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    is_authorized: bool = Depends(is_admin)
):
    """
    Overwrites existing data with data from new source
    """
    pass


@router.delete(
    "/{dataset}/{version}/sources",
    response_class=ORJSONResponse,
    tags=["Sources"],
    response_model=Source,
)
async def delete_sources(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    is_authorized: bool = Depends(is_admin)
):
    """
    Deletes existing data
    """
    pass
