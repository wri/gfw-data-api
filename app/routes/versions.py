from typing import List

from asyncpg.exceptions import UniqueViolationError
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import ORJSONResponse

from app.routes import dataset_dependency, version_dependency, update_metadata

from ..models.orm.asset import Asset as ORMAsset
from ..models.orm.version import Version as ORMVersion
from ..models.pydantic.version import Version, VersionCreateIn, VersionUpdateIn

router = APIRouter()


@router.get("/{dataset}/{version}", response_class=ORJSONResponse, tags=["Version"])
async def get_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency)
):
    """
    Get basic metadata for a given version
    """
    row: ORMVersion = await ORMVersion.get([dataset, version])
    assets: List[ORMAsset] = await ORMAsset.query.where(ORMAsset.dataset == dataset).where(ORMAsset.version == version).gino.all()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Version with name {dataset}/{version} does not exist")
    response = Version.from_orm(row).dict(by_alias=True)
    response["assets"] = assets
    return response


@router.put("/{dataset}/{version}", response_class=ORJSONResponse, tags=["Version"])
async def put_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    request: VersionCreateIn
):
    """
    Create or update a version for a given dataset
    """
    try:
        new_version: ORMVersion = await ORMVersion.create(dataset=dataset, version=version, **request.dict())
    except UniqueViolationError:
        raise HTTPException(status_code=400, detail=f"Dataset with name {dataset} already exists")

    return Version.from_orm(new_version)


@router.patch("/{dataset}/{version}", response_class=ORJSONResponse, tags=["Version"])
async def patch_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
    request: VersionUpdateIn
):
    """
    Partially update a version of a given dataset
    """
    row: ORMVersion = await ORMVersion.get([dataset, version])

    if row is None:
        raise HTTPException(status_code=404, detail=f"Version with name {dataset}/{version} does not exists")

    row = await update_metadata(row, request, Version)

    return Version.from_orm(row)


@router.delete("/{dataset}/{version}", response_class=ORJSONResponse, tags=["Version"])
async def delete_version(
    *,
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency)
):
    """
    Delete a version
    """
    row: ORMVersion = await ORMVersion.get([dataset, version])
    await ORMVersion.delete.where(ORMVersion.dataset == dataset).where(ORMVersion.version == version).gino.status()

    # TODO: Delete all assets

    return Version.from_orm(row)
