from datetime import datetime
from typing import List
from uuid import UUID

from fastapi import Query

from app.models.pydantic.base import BaseRecord, StrictBaseModel
from app.models.pydantic.responses import Response


class APIKeyRequestIn(StrictBaseModel):

    organization: str = Query(..., description="Name of organization or Website")
    email: str = Query(..., description="Email address of POC")
    domains: List[str] = Query(
        ...,
        description="List of domains which can be used this API key. "
        "When making request using the API key, make sure you add the correct `orgin` header matching a whitelisted domain."
        "You can use wildcards for subdomains such as *.yourdomain.com. "
        "Our validation methoerd for wildcard will allow only subdomains. So make sure you also add yourdomain.com if you use root without any subdomains."
        "www.yourdomain.com and yourdomain.com are two different domains in terms of security. Include www. if required.",
    )


class ApiKey(BaseRecord):
    api_key: UUID
    organization: str
    email: str
    domains: List[str]
    expiration_date: datetime


class ApiKeyResponse(Response):
    data: ApiKey
