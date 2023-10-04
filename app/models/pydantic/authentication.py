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

    alias: str = Query(..., description="Nickname for API Key")
    organization: str = Query(..., description="Name of organization or website")
    email: EmailStr = Query(..., description="Email address of POC")
    domains: List[str] = Query(
        [],
        description="""List of domains which can be used this API key.
        If no domain is listed, the key will be set by default to the lowest rate
        limiting tier. <br/>
        When making request using the API key, make sure you add the correct `origin`
        header matching a domain in this allowlist.<br/><br/>
        You can use wildcards for subdomains such as `*.yourdomain.com`.<br/>
        **Our validation method for wildcards will allow only subdomains.**<br/><br/>
        Make sure you also add `yourdomain.com` if you use root without any subdomains.<br/>
        `www.yourdomain.com` and `yourdomain.com` are two different domains in terms
        of security.<br/>
        Include `www.` if required.<br/><br/>
        **Do not** include port numbers in the domain names. `localhost`~:3000~<br/><br/>
        A `domains` example for local development might look like this:<br/>
        `["www.yourdomain.com", "*.yourdomain.com", "yourdomain.com", "localhost"]`""",
        regex=r"^(\*\.)?([\w-]+\.)+[\w-]+$|^(localhost)$",
    )
    never_expires: bool = Query(
        False,
        description="Set API Key to never expire, only `admin` users can set this to `true`",
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
