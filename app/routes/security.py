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
        f"Requesting Bearer token from GFW production API for user {form_data.username}"
    )
    url = "https://production-api.globalforestwatch.org/auth/login"

    response = requests.post(url, data=json.dumps(payload), headers=headers)

    if response.status_code != 200:
        logger.warning(
            f"Authentication for user {form_data.username} failed. API responded with status code {response.status_code} and message {response.text}"
        )
        raise HTTPException(status_code=401, detail="Authentication failed")

    else:
        return {
            "access_token": response.json()["data"]["token"],
            "token_type": "bearer",
        }
