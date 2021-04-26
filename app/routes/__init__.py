from typing import Tuple

from fastapi import Depends, HTTPException, Path
from fastapi.security import OAuth2PasswordBearer

from ..crud.versions import get_version
from ..errors import RecordNotFoundError

DATASET_REGEX = r"^[a-z][a-z0-9_-]{2,}$"
VERSION_REGEX = r"^v\d{1,8}(\.\d{1,3}){0,2}?$|^latest$"
DATE_REGEX = r"^\d{4}(\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01]))?$"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")


async def dataset_dependency(
    dataset: str = Path(..., title="Dataset", regex=DATASET_REGEX)
) -> str:
    if dataset == "latest":
        raise HTTPException(
            status_code=400,
            detail="Name `latest` is reserved for versions only.",
        )
    return dataset


async def version_dependency(
    version: str = Path(..., title="Dataset version", regex=VERSION_REGEX),
) -> str:
    # Middleware should have redirected GET requests to latest version already.
    # Any other request method should not use `latest` keyword.
    if version == "latest":
        raise HTTPException(
            status_code=400,
            detail="You must list version name explicitly for this operation.",
        )
    return version


async def dataset_version_dependency(
    dataset: str = Depends(dataset_dependency),
    version: str = Depends(version_dependency),
) -> Tuple[str, str]:
    # make sure version exists
    try:
        await get_version(dataset, version)
    except RecordNotFoundError as e:
        raise HTTPException(status_code=404, detail=(str(e)))

    return dataset, version
