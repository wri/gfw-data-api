import json
import logging

import requests

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm

router = APIRouter()


@router.post("/token", tags=["Authentication"])
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    headers = {"Content-Type": "application/json"}
    payload = {"email": form_data.username, "password": form_data.password}

    url = f"https://production-api.globalforestwatch.org/auth/login"

    response = requests.post(url, data=json.dumps(payload), headers=headers)
    logging.warning(response.text)
    if response.status_code != 200:

        raise HTTPException(status_code=400, detail="Authentication failed")

    else:
        return {
            "access_token": response.json()["data"]["token"],
            "token_type": "bearer",
        }
