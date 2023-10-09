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


# Check is the authorized user is an admin. Return true if so, throw
# an exception if not.
async def is_admin(token: str = Depends(oauth2_scheme)) -> bool:
    """Calls GFW API to authorize user.

    User must be ADMIN for gfw app
    """

    response: Response = await who_am_i(token)

    if response.status_code == 401 or not (
        response.json()["role"] == "ADMIN"
        and "gfw" in response.json()["extraUserData"]["apps"]
    ):
        logger.warning(f"ADMIN privileges required. Unauthorized user: {response.text}")
        raise HTTPException(status_code=401, detail="Unauthorized")
    else:
        return True

# Check is the authorized user is an admin. Return true if so, false if not (with no
# exception).
async def is_admin_no_exception(token: str = Depends(oauth2_scheme)) -> bool:
    """Calls GFW API to authorize user.

    User must be ADMIN for gfw app
    """

    response: Response = await who_am_i(token)

    if response.status_code == 401 or not (
        response.json()["role"] == "ADMIN"
        and "gfw" in response.json()["extraUserData"]["apps"]
    ):
        logger.warning(f"ADMIN privileges required. Unauthorized user: {response.text}")
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
