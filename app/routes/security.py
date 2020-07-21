from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm

from ..errors import UnauthorizedError
from ..utils.rw_api import login

router = APIRouter()


@router.post("/token", tags=["Authentication"])
async def get_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """Get access token from RW API."""
    try:
        token = await login(form_data.username, form_data.password)
    except UnauthorizedError as e:
        raise HTTPException(status_code=401, detail=str(e))

    else:
        return {
            "access_token": token,
            "token_type": "bearer",
        }
