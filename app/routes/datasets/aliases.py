"""Alias for dataset version.

This allows to get dataset version by alias from
`/dataset/{dataset}/{version}` endpoing using `version_alias` as the
`version` parameter.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, status
from fastapi.responses import ORJSONResponse

from ...authentication.token import is_admin
from ...crud import aliases as crud
from ...crud.versions import get_version
from ...errors import RecordAlreadyExistsError, RecordNotFoundError
from ...models.orm.aliases import Alias as ORMAlias
from ...models.pydantic.aliases import Alias, AliasCreateIn, AliasResponse
from ...routes import VERSION_REGEX, dataset_dependency

router = APIRouter()


@router.get(
    "/version/{dataset}/{version_alias}",
    response_class=ORJSONResponse,
    tags=["Aliases"],
    response_model=AliasResponse,
    include_in_schema=True,  # TODO switch to false before production to hide from docs
)
async def get_alias(
    *,
    dataset: str = Depends(dataset_dependency),
    version_alias: str = Path(..., title="Version alias", regex=VERSION_REGEX),
    is_authorized: bool = Depends(is_admin),
):
    """Get version alias for a dataset."""

    try:
        alias: ORMAlias = await crud.get_alias(dataset, version_alias)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return AliasResponse(data=Alias.from_orm(alias))


@router.put(
    "/version/{dataset}/{version_alias}",
    response_class=ORJSONResponse,
    tags=["Aliases"],
    response_model=AliasResponse,
    include_in_schema=True,
    status_code=status.HTTP_200_OK,
)
async def add_new_alias(
    *,
    dataset: str = Depends(dataset_dependency),
    version_alias: str = Path(..., title="Version alias", regex=VERSION_REGEX),
    request: AliasCreateIn,
    is_authorized: bool = Depends(is_admin),
):
    """Add a new version alias for a dataset."""

    input_data = request.dict(exclude_none=True, by_alias=True)

    # avoid conflict with existing version with name `version_alias`
    try:
        version = await get_version(dataset, version_alias)
        if version is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The alias is already used for a dataset version.",
            )
    except RecordNotFoundError:
        pass

    try:
        alias: ORMAlias = await crud.create_alias(
            version_alias, dataset, input_data["version"]
        )
    except (RecordAlreadyExistsError, RecordNotFoundError) as e:
        raise HTTPException(status_code=400, detail=str(e))

    return AliasResponse(data=Alias.from_orm(alias))


@router.delete(
    "/version/{dataset}/{version_alias}",
    response_class=ORJSONResponse,
    tags=["Aliases"],
    response_model=AliasResponse,
    status_code=status.HTTP_200_OK,
    include_in_schema=True,
)
async def delete_alias(
    *,
    dataset: str = Depends(dataset_dependency),
    version_alias: str = Path(..., title="Version alias", regex=VERSION_REGEX),
    is_authorized: bool = Depends(is_admin),
):
    """Delete a dataset's version alias."""

    try:
        alias: ORMAlias = await crud.delete_alias(dataset, version_alias)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return AliasResponse(data=Alias.from_orm(alias))
