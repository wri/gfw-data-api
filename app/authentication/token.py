from typing import Tuple, cast

from fastapi import Depends, HTTPException
from fastapi.logger import logger
from fastapi.security import OAuth2PasswordBearer
from httpx import Response
from ..routes import dataset_dependency

from ..utils.rw_api import who_am_i
from ..settings.globals import PROTECTED_QUERY_DATASETS


def get_request_token() -> str:
    """ Get request token if it exists """
    return str(OAuth2PasswordBearer(tokenUrl="/token"))


def get_request_token_optional() -> str:
    """ Get request token if it exists, error if not """
    return str(OAuth2PasswordBearer(tokenUrl="/token", auto_error=False))


async def get_request_user() -> Tuple[str, str]:
    token: str = get_request_token()
    user_id, role = await get_user(token)
    return user_id, role


async def is_service_account(token: str = get_request_token()) -> bool:
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


async def is_admin(token: str = get_request_token()) -> bool:
    """Calls GFW API to authorize user.

    User must be ADMIN for gfw app
    """

    return await is_app_admin(token, "gfw", "Unauthorized")

async def is_gfwpro_admin_for_query(dataset: str = Depends(dataset_dependency),
                                    token: str | None = get_request_token_optional()) -> bool:
    """If the dataset is protected dataset, calls GFW API to authorize user by
    requiring the user must be ADMIN for gfw-pro app.  If the dataset is not
    protected, just returns True without any required token or authorization.
    """
    
    if dataset in PROTECTED_QUERY_DATASETS:
        if token == None:
            raise HTTPException(status_code=401, detail="Unauthorized query on a restricted dataset")
        else:
            return await is_app_admin(cast(str, token), "gfw-pro",
                               error_str="Unauthorized query on a restricted dataset")

    return True

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


async def get_user(token: str = get_request_token()) -> Tuple[str, str]:
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
