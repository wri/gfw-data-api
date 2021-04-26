from datetime import datetime
from typing import List
from uuid import UUID

from fastapi import Query
from pydantic import EmailStr

from app.models.pydantic.base import BaseRecord, StrictBaseModel
from app.models.pydantic.responses import Response


class APIKeyRequestIn(StrictBaseModel):

    organization: str = Query(..., description="Name of organization or Website")
    email: EmailStr = Query(..., description="Email address of POC")
    domains: List[str] = Query(
        ...,
        description="List of domains which can be used this API key. "
        "When making request using the API key, make sure you add the correct `orgin` header matching a whitelisted domain."
        "You can use wildcards for subdomains such as *.yourdomain.com. "
        "Our validation methoerd for wildcard will allow only subdomains. So make sure you also add yourdomain.com if you use root without any subdomains."
        "www.yourdomain.com and yourdomain.com are two different domains in terms of security. Include www. if required.",
        regex=r"^(\*\.)?([\w-]+\.)+[\w-]+$|(localhost)",
    )


class ApiKey(BaseRecord):
    user_id: str
    api_key: UUID
    organization: str
    email: str
    domains: List[str]
    expires_on: datetime


class ApiKeyResponse(Response):
    data: ApiKey


class ApiKeysResponse(Response):
    data: List[ApiKey]
