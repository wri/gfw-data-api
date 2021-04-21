from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm

from ..authentication.token import is_admin
from ..crud import api_keys
from ..errors import UnauthorizedError
from ..models.orm.api_keys import ApiKey as ORMApiKey
from ..models.pydantic.security import APIKeyRequestIn, ApiKeyResponse
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


@router.post("/apikey", tags=["Authentication"])
async def create_apikey(
    request: APIKeyRequestIn,
    is_authorized: bool = Depends(is_admin),
):
    """Request a new API key.

    Default keys are valid for one year
    """

    input_data = request.dict(exclude_none=True, by_alias=True)

    row: ORMApiKey = await api_keys.create_api_key(**input_data)

    return ApiKeyResponse(data=row)
