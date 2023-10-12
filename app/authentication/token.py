from typing import Tuple

from fastapi import Depends, HTTPException
from fastapi.logger import logger
from fastapi.security import OAuth2PasswordBearer
from httpx import Response

from ..utils.rw_api import who_am_i

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")


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

    return await is_app_admin(token, "gfw", "Unauthorized")

async def is_gfwpro_admin(error_str: str, token: str = Depends(oauth2_scheme)) -> bool:
    """Calls GFW API to authorize user.

    User must be ADMIN for gfw pro app
    """

    return await is_app_admin(token, "gfw-pro", error_str)

async def is_app_admin(token: str, app: str, error_str: str) -> bool:
    """Calls GFW API to authorize user.

    User must be an ADMIN for the specified app, else it will throw
    an exception with the specified error string.
    """

    response: Response = await who_am_i(token)

    if response.status_code == 401 or not (
        response.json()["role"] == "ADMIN"
        and app in response.json()["extraUserData"]["apps"]
    ):
        logger.warning(f"ADMIN privileges required. Unauthorized user: {response.text}")
        raise HTTPException(status_code=401, detail=error_str)
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
