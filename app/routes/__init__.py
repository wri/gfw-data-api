from typing import Tuple

import requests
from fastapi import Depends, Form, HTTPException, Path
from fastapi.logger import logger
from fastapi.security import OAuth2PasswordBearer

from app.crud.versions import get_latest_version

VERSION_REGEX = r"^v\d{1,8}\.?\d{1,3}\.?\d{1,3}$|^latest$"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")


async def dataset_dependency(dataset: str = Path(..., title="Dataset")) -> str:
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


# async def version_dependency_form(
#     version: str = Form(..., title="Dataset version", regex=VERSION_REGEX)
# ):
#
#     if version == "latest":
#          version = await get_latest_version
#
#     return version


async def is_admin(token: str = Depends(oauth2_scheme)) -> bool:
    """
    Calls GFW API to authorize user
    """

    headers = {"Authorization": f"Bearer {token}"}
    url = "https://production-api.globalforestwatch.org/auth/check-logged"
    response = requests.get(url, headers=headers)

    if response.status_code != 200 and response.status_code != 401:
        logger.warning(
            f"Failed to authorize user. Server responded with response code: {response.status_code} and message: {response.text}"
        )
        raise HTTPException(
            status_code=500, detail="Call to authorization server failed"
        )

    if response.status_code == 401 or not (
        response.json()["role"] == "ADMIN"
        and "gfw" in response.json()["extraUserData"]["apps"]
    ):
        logger.info("Unauthorized user")
        raise HTTPException(status_code=401, detail="Unauthorized")
    else:
        return True
