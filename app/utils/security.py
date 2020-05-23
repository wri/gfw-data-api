import requests
from fastapi import Depends, HTTPException
from fastapi.logger import logger
from fastapi.security import OAuth2PasswordBearer

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")


async def is_authorized(token: str = Depends(oauth2_scheme)) -> bool:
    """
    Calls GFW API to authorize user
    """

    logger.debug("Verify if token is valid")
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://production-api.globalforestwatch.org/auth/check-logged"
    response = requests.get(url, headers=headers)

    if response.status_code != 200 and response.status_code != 401:
        logger.warning(
            f"Failed to verify token. GFW API responded with status code {response.status_code} and message: {response.text}"
        )
        raise HTTPException(
            status_code=500, detail="Call to authorization server failed"
        )

    if response.status_code == 401 or not (
        response.json()["role"] == "ADMIN"
        and "gfw" in response.json()["extraUserData"]["apps"]
    ):
        logger.info("Token is not valid")
        raise HTTPException(status_code=401, detail="Unauthorized")
    else:
        return True
