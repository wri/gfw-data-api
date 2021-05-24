from typing import List, Optional, Tuple
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.security import OAuth2PasswordRequestForm

from ...authentication.api_keys import api_key_is_valid
from ...authentication.token import get_user, is_admin
from ...crud import api_keys
from ...errors import RecordNotFoundError, UnauthorizedError
from ...models.orm.api_keys import ApiKey as ORMApiKey
from ...models.pydantic.authentication import (
    ApiKey,
    APIKeyRequestIn,
    ApiKeyResponse,
    ApiKeysResponse,
    ApiKeyValidation,
    ApiKeyValidationResponse,
    SignUpRequestIn,
    SignUpResponse,
)
from ...models.pydantic.responses import Response
from ...utils.rw_api import login, signup

router = APIRouter()


@router.post("/sign-up", tags=["Authentication"])
async def sign_up(request: SignUpRequestIn):
    data = await signup(request.name, request.email)
    return SignUpResponse(data=data)


@router.post("/token", tags=["Authentication"])
async def get_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """Get access token from RW API."""
    try:
        token = await login(form_data.username, form_data.password)
    except UnauthorizedError as e:
        raise HTTPException(status_code=401, detail=str(e))

    else:
        return Response(
            data={
                "access_token": token,
                "token_type": "bearer",
            }
        )


@router.post("/apikey", tags=["Authentication"], status_code=201)
async def create_apikey(
    request: APIKeyRequestIn,
    user: Tuple[str, str] = Depends(get_user),
):
    """Request a new API key.

    Default keys are valid for one year
    """

    user_id, user_role = user
    if len(request.domains) == 0 and user_role != "ADMIN":
        raise HTTPException(
            status_code=400,
            detail=f"Users with role {user_role} must list at least one domain.",
        )

    if request.never_expires and user_role != "ADMIN":
        raise HTTPException(
            status_code=400,
            detail=f"Users with role {user_role} cannot set `never_expires` to True.",
        )

    input_data = request.dict(by_alias=True)

    row: ORMApiKey = await api_keys.create_api_key(user_id=user_id, **input_data)

    return ApiKeyResponse(data=row)


@router.get("/apikey/{api_key}", tags=["Authentication"])
async def get_apikey(
    api_key: UUID = Path(..., description="API Key"),
    user: Tuple[str, str] = Depends(get_user),
):
    """Request a new API key.

    Default keys are valid for one year
    """
    user_id, role = user
    try:
        row: ORMApiKey = await api_keys.get_api_key(api_key)
    except RecordNotFoundError:
        raise HTTPException(status_code=404, detail="The API Key does not exist.")

    if role != "ADMIN" and row.user_id != user_id:
        raise HTTPException(
            status_code=403, detail="API Key is not associate with current user."
        )

    data = ApiKey.from_orm(row)

    return ApiKeyResponse(data=data)


@router.get("/apikeys", tags=["Authentication"])
async def get_apikeys(
    user: Tuple[str, str] = Depends(get_user),
):
    """Request a new API key.

    Default keys are valid for one year
    """
    user_id, _ = user
    rows: List[ORMApiKey] = await api_keys.get_api_keys_from_user(user_id)
    data = [ApiKey.from_orm(row) for row in rows]

    return ApiKeysResponse(data=data)


@router.get("/apikey/{api_key}/validate", tags=["Authentication"])
async def validate_apikey(
    api_key: UUID = Path(
        ..., description="Api Key to delete. Must be owned by authenticated user."
    ),
    origin: Optional[str] = Query(None, description="Origin used with API Key"),
    referrer: Optional[str] = Query(
        None, description="Referrer of call used with API Key"
    ),
    is_authorized: bool = Depends(is_admin),
):
    """Check if a given API key is valid."""
    try:
        row: ORMApiKey = await api_keys.get_api_key(api_key)
    except RecordNotFoundError:
        raise HTTPException(
            status_code=404, detail="The requested API key does not exists."
        )

    data = ApiKeyValidation(
        is_valid=api_key_is_valid(row.domains, row.expires_on, origin, referrer)
    )
    return ApiKeyValidationResponse(data=data)


@router.delete("/apikey/{api_key}", tags=["Authentication"])
async def delete_apikey(
    api_key: UUID = Path(
        ..., description="Api Key to delete. Must be owned by authenticated user."
    ),
    user: Tuple[str, str] = Depends(get_user),
):
    """Delete existing API key.

    API Key must belong to user.
    """
    user_id, _ = user
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
