from typing import cast, Tuple, List

from fastapi import Depends, HTTPException
from fastapi.logger import logger
from fastapi.security import OAuth2PasswordBearer
from httpx import Response

from ..models.pydantic.authentication import User
from ..routes import dataset_version_dependency
from ..settings.globals import PROTECTED_QUERY_DATASETS, PROTECTED_QUERY_DATASET_VERSIONS
from ..utils.rw_api import who_am_i

# token dependency where we immediately cause an exception if there is no auth token
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")
# token dependency where we don't cause exception if there is no auth token
oauth2_scheme_no_auto = OAuth2PasswordBearer(tokenUrl="/token", auto_error=False)


async def is_service_account(token: str = Depends(oauth2_scheme)) -> bool:
    """Calls GFW API to authorize user.

    User must be service account with email gfw-sync@wri.org
    """

    response: Response = await who_am_i(token)

    if response.status_code == 401 or not (
        response.json()["email"] == "gfw-sync@wri.org"
        and "gfw" in response.json()["extraUserData"]["apps"]
    ):
        logger.info("Unauthorized user")
        raise HTTPException(status_code=401, detail="Unauthorized")
    else:
        return True


async def is_admin(token: str = Depends(oauth2_scheme)) -> bool:
    """Calls GFW API to authorize user.

    User must be ADMIN for gfw app
    """

    return await is_app_admin(token, ["gfw"], "Unauthorized")


async def is_authorized_for_query(
    dv: Tuple[str, str] = Depends(dataset_version_dependency),
    token: str | None = Depends(oauth2_scheme_no_auto),
) -> bool:
    """If the dataset is protected dataset or the dataset/version is a protected version, calls GFW API
    to authorize user by requiring the user must be ADMIN for gfw or gfw-pro app.

    If the dataset is not protected, just returns True without any
    required token or authorization.
    """

    dataset, version = dv
    if dataset in PROTECTED_QUERY_DATASETS or f"{dataset}/{version}" in PROTECTED_QUERY_DATASET_VERSIONS:
        if token is None:
            raise HTTPException(
                status_code=401, detail="Unauthorized query on a restricted dataset or version"
            )
        else:
            return await is_app_admin(
                cast(str, token),
                ["gfw", "gfw-pro"],
                error_str="Unauthorized query on a restricted dataset or version",
            )

    return True


async def is_app_admin(token: str, apps: List[str], error_str: str) -> bool:
    """Calls GFW API to authorize user.

    User must be an ADMIN for one of the specified apps, else it will throw an
    exception with the specified error string.
    """

    response: Response = await who_am_i(token)

    if response.status_code == 401 or not (
        response.json()["role"] == "ADMIN"
        and any(app in response.json()["extraUserData"]["apps"] for app in apps)
    ):
        logger.warning(f"ADMIN privileges required. Unauthorized user: {response.text}")
        raise HTTPException(status_code=401, detail=error_str)
    else:
        return True


async def get_user(token: str = Depends(oauth2_scheme)) -> User:
    """Get the details for authenticated user."""

    response: Response = await who_am_i(token)

    if response.status_code == 401:
        logger.info("Unauthorized user")
        raise HTTPException(status_code=401, detail="Unauthorized access - this operation requires user authentication via a token")
    else:
        return User(**response.json())


async def get_admin(user: User = Depends(get_user)) -> User:
    """Get the details for authenticated ADMIN user."""

    if user.role != "ADMIN":
        raise HTTPException(status_code=401, detail="Unauthorized access - this operation requires authentication as a user that is an admin")

    return user


async def get_manager(user: User = Depends(get_user)) -> User:
    """Get the details for authenticated MANAGER for data-api application or
    ADMIN user."""

    if user.role != "ADMIN" and user.role != "MANAGER":
        raise HTTPException(status_code=401, detail="Unauthorized write access to a dataset/version/asset by a user who is not an admin or data manager")

    return user
