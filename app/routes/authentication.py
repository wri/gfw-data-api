from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.security import OAuth2PasswordRequestForm

from ..authentication.token import get_user_id
from ..crud import api_keys
from ..errors import RecordNotFoundError, UnauthorizedError
from ..models.orm.api_keys import ApiKey as ORMApiKey
from ..models.pydantic.authentication import (
    ApiKey,
    APIKeyRequestIn,
    ApiKeyResponse,
    ApiKeysResponse,
)
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


@router.post("/apikey", tags=["Authentication"], status_code=201)
async def create_apikey(
    request: APIKeyRequestIn,
    user_id: str = Depends(get_user_id),
):
    """Request a new API key.

    Default keys are valid for one year
    """

    input_data = request.dict(exclude_none=True, by_alias=True)

    row: ORMApiKey = await api_keys.create_api_key(user_id=user_id, **input_data)

    return ApiKeyResponse(data=row)


@router.get("/apikeys", tags=["Authentication"])
async def get_apikeys(
    user_id: str = Depends(get_user_id),
):
    """Request a new API key.

    Default keys are valid for one year
    """

    rows: List[ORMApiKey] = await api_keys.get_api_keys_from_user(user_id)
    data = [ApiKey.from_orm(row) for row in rows]

    return ApiKeysResponse(data=data)


@router.delete("/apikey/{api_key}", tags=["Authentication"])
async def delete_apikey(
    api_key: UUID = Path(
        ..., description="Api Key to delete. Must be owned by authenticated user."
    ),
    user_id: str = Depends(get_user_id),
):
    """Delete existing API key.

    API Key must belong to user.
    """
    try:
        row: ORMApiKey = await api_keys.get_api_key(api_key)
    except RecordNotFoundError:
        raise HTTPException(
            status_code=404, detail="The requested API key does not exists."
        )

    # TODO: we might want to allow admins to delete api keys of other users?
    if not row.user_id == user_id:
        raise HTTPException(
            status_code=403,
            detail="The requested API key does not belong to the current user.",
        )

    row = await api_keys.delete_api_key(api_key)

    return ApiKeyResponse(data=ApiKey.from_orm(row))
