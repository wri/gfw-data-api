from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import Query
from pydantic import EmailStr

from app.models.pydantic.base import BaseRecord, StrictBaseModel
from app.models.pydantic.responses import Response


class SignUpRequestIn(StrictBaseModel):
    name: str = Query(..., description="Full user name")
    email: EmailStr = Query(..., description="User's email address")


class SignUp(StrictBaseModel):
    id: str
    name: str
    email: EmailStr
    createdAt: datetime
    role: str
    extraUserData: Dict[str, Any]


class SignUpResponse(Response):
    data: SignUp


class APIKeyRequestIn(StrictBaseModel):

    alias: str = Query(..., description="Nick name for API Key")
    organization: str = Query(..., description="Name of organization or Website")
    email: EmailStr = Query(..., description="Email address of POC")
    domains: List[str] = Query(
        [],
        description="List of domains which can be used this API key. If no domain is listed, the key will be set by default to the lowest rate limiting tier. "
        "When making request using the API key, make sure you add the correct `origin` header matching a whitelisted domain. "
        "You can use wildcards for subdomains such as *.yourdomain.com. "
        "Our validation methord for wildcard will allow only subdomains. So make sure you also add yourdomain.com if you use root without any subdomains. "
        "www.yourdomain.com and yourdomain.com are two different domains in terms of security. Include www. if required. ",
        regex=r"^(\*\.)?([\w-]+\.)+[\w-]+$|(localhost)",
    )
    never_expires: bool = Query(
        False,
        description="Set API Key to never expire, only admin uses can set this to True",
    )


class ApiKey(BaseRecord):
    alias: Optional[str]
    user_id: str
    api_key: UUID
    organization: str
    email: str
    domains: List[str]
    expires_on: Optional[datetime]


class ApiKeyValidation(StrictBaseModel):
    is_valid: bool


class ApiKeyResponse(Response):
    data: ApiKey


class ApiKeysResponse(Response):
    data: List[ApiKey]


class ApiKeyValidationResponse(Response):
    data: ApiKeyValidation
