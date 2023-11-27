from typing import List, Optional, Tuple
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from fastapi.security import OAuth2PasswordRequestForm

from ...authentication.api_keys import api_key_is_internal, api_key_is_valid
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
from ...settings.globals import (
    API_GATEWAY_EXTERNAL_USAGE_PLAN,
    API_GATEWAY_ID,
    API_GATEWAY_INTERNAL_USAGE_PLAN,
    API_GATEWAY_STAGE_NAME,
)
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
async def create_api_key(
    api_key_data: APIKeyRequestIn,
    request: Request,
    user: Tuple[str, str] = Depends(get_user),
):
    """Request a new API key.

    Default keys are valid for one year
    """

    user_id, user_role = user

    if api_key_data.never_expires and user_role != "ADMIN":
        raise HTTPException(
            status_code=400,
            detail=f"Users with role {user_role} cannot set `never_expires` to True.",
        )

    input_data = api_key_data.dict(by_alias=True)

    origin = request.headers.get("origin")
    referrer = request.headers.get("referer")
    if not api_key_is_valid(input_data["domains"], origin=origin, referrer=referrer):
        raise HTTPException(
            status_code=400,
            detail="Domain name did not match the request origin or referrer.",
        )

    # Give a good error code/message if user is specifying an alias that exists for
    # another one of his API keys.
    prev_keys: List[ORMApiKey] = await api_keys.get_api_keys_from_user(user_id=user_id)
    for key in prev_keys:
        if key.alias == api_key_data.alias:
            raise HTTPException(
                status_code=409,
                detail="Key with specified alias already exists; use a different alias"
            )

    row: ORMApiKey = await api_keys.create_api_key(user_id=user_id, **input_data)

    is_internal = api_key_is_internal(
        api_key_data.domains, user_id=None, origin=origin, referrer=referrer
    )
    usage_plan_id = (
        API_GATEWAY_INTERNAL_USAGE_PLAN
        if is_internal is True
        else API_GATEWAY_EXTERNAL_USAGE_PLAN
    )
    await api_keys.add_api_key_to_gateway(
        row.alias,
        str(row.api_key),
        API_GATEWAY_ID,
        API_GATEWAY_STAGE_NAME,
        usage_plan_id,
    )

    return ApiKeyResponse(data=row)


@router.get("/apikey/{api_key}", tags=["Authentication"])
async def get_api_key(
    api_key: UUID = Path(..., description="API Key"),
    user: Tuple[str, str] = Depends(get_user),
):
    """Get details for a specific API Key.

    User must own API Key or must be Admin to see details.
    """
    user_id, role = user
    try:
        row: ORMApiKey = await api_keys.get_api_key(api_key)
    except RecordNotFoundError:
        raise HTTPException(status_code=404, detail="The API Key does not exist.")

    if role != "ADMIN" and row.user_id != user_id:
        raise HTTPException(
            status_code=403, detail="API Key is not associated with current user."
        )

    data = ApiKey.from_orm(row)

    return ApiKeyResponse(data=data)


@router.get("/apikeys", tags=["Authentication"])
async def get_api_keys(
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
async def validate_api_key(
    api_key: UUID = Path(
        ..., description="Api Key to validate. Must be owned by authenticated user."
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
            status_code=404, detail="The requested API key does not exist."
        )

    data = ApiKeyValidation(
        is_valid=api_key_is_valid(row.domains, row.expires_on, origin, referrer)
    )
    return ApiKeyValidationResponse(data=data)


@router.delete("/apikey/{api_key}", tags=["Authentication"])
async def delete_api_key(
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
            status_code=404, detail="The requested API key does not exist."
        )

    # TODO: we might want to allow admins to delete api keys of other users?
    if not row.user_id == user_id:
        raise HTTPException(
            status_code=403,
            detail="The requested API key does not belong to the current user.",
        )

    row = await api_keys.delete_api_key(api_key)
    try:
        await api_keys.delete_api_key_from_gateway(name=row.alias)
    except RecordNotFoundError:
        raise HTTPException(
            status_code=404, detail="The request API key does not exist."
        )

    return ApiKeyResponse(data=ApiKey.from_orm(row))
