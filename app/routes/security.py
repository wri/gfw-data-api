import json

import requests
from fastapi import APIRouter, Depends, HTTPException
from fastapi.logger import logger
from fastapi.security import OAuth2PasswordRequestForm

router = APIRouter()


@router.post("/token", tags=["Authentication"])
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    headers = {"Content-Type": "application/json"}
    payload = {"email": form_data.username, "password": form_data.password}

    logger.debug(
        f"Calling GFW production API for login token for user {form_data.username}"
    )
    url = "https://production-api.globalforestwatch.org/auth/login"

    response = requests.post(url, data=json.dumps(payload), headers=headers)
    logger.warning(response.text)
    if response.status_code != 200:

        raise HTTPException(status_code=400, detail="Authentication failed")

    else:
        return {
            "access_token": response.json()["data"]["token"],
            "token_type": "bearer",
        }
