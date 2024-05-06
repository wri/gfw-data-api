from typing import Tuple, cast

from fastapi import Depends, HTTPException
from fastapi.logger import logger
from fastapi.security import OAuth2PasswordBearer
from httpx import Response

from ..routes import dataset_dependency
from ..settings.globals import PROTECTED_QUERY_DATASETS
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


async def assert_admin(token: str = Depends(oauth2_scheme)) -> None:
    """Calls GFW API to authorize user.

    User must be ADMIN for gfw app
    """

    return await assert_app_role(token, "ADMIN", "gfw", "Unauthorized")


async def assert_manager(token: str = Depends(oauth2_scheme)) -> None:
    """Calls GFW API to authorize user.

    User must be MANAGER for data-api app.
    """

    return await assert_app_role(token, "MANAGER", "data-api", "Unauthorized")


async def is_admin_or_manager(token: str = Depends(oauth2_scheme)) -> bool:
    """Calls GFW API to authorize user.

    User must be ADMIN for gfw app or MANAGER for data-api app.
    """

    return (await assert_admin(token)) or (await assert_manager(token))


async def rw_user_id(token: str = Depends(oauth2_scheme)) -> str:
    """Gets user ID from token."""

    return await who_am_i(token).json()["id"]


async def is_gfwpro_admin_for_query(
    dataset: str = Depends(dataset_dependency),
    token: str | None = Depends(oauth2_scheme_no_auto),
) -> bool:
    """If the dataset is protected dataset, calls GFW API to authorize user by
    requiring the user must be ADMIN for gfw-pro app.

    If the dataset is not protected, just returns True without any
    required token or authorization.
    """

    if dataset in PROTECTED_QUERY_DATASETS:
        if token is None:
            raise HTTPException(
                status_code=401, detail="Unauthorized query on a restricted dataset"
            )
        else:
            return await is_app_role(
                cast(str, token),
                "ADMIN",
                "gfw-pro",
                error_str="Unauthorized query on a restricted dataset",
            )

    return True


async def assert_app_role(token: str, role: str, app: str, error_str: str) -> None:
    is_authorized = await is_app_role(token, role, app)

    if not is_authorized:
        raise HTTPException(status_code=401, detail=error_str)


async def is_app_role(token: str, role: str, app: str) -> bool:
    """Calls RW API to authorize user for specific role and app."""

    response: Response = await who_am_i(token)

    if response.status_code == 401 or not (
        response.json()["role"] == role
        and app in response.json()["extraUserData"]["apps"]
    ):
        return False
    else:
        return True


async def get_user(token: str = Depends(oauth2_scheme)) -> Tuple[str, str]:
    """Calls GFW API to authorize user.

    This functions check is user of any level is associated with the GFW
    app and returns the user ID
    """

    response: Response = await who_am_i(token)

    if response.status_code == 401 or not (
        "gfw" in response.json()["extraUserData"]["apps"]
    ):
        logger.info("Unauthorized user")
        raise HTTPException(status_code=401, detail="Unauthorized")
    else:
        return response.json()["id"], response.json()["role"]
