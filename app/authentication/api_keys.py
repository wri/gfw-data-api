import re
from datetime import datetime
from typing import List, Optional, Tuple
from urllib.parse import urlparse
from uuid import UUID

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader, APIKeyQuery
from starlette.status import HTTP_403_FORBIDDEN

from ..crud import api_keys
from ..errors import RecordNotFoundError
from ..models.orm.api_keys import ApiKey as ORMApiKey
from ..settings.globals import API_KEY_NAME


class APIKeyOriginQuery(APIKeyQuery):
    async def __call__(
        self, request: Request
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        api_key: str = request.query_params.get(self.model.name)
        origin: Optional[str] = request.headers.get("origin")
        referrer: Optional[str] = request.headers.get("referer")  # !sic
        return _api_key_origin_auto_error(api_key, origin, referrer, self.auto_error)


class APIKeyOriginHeader(APIKeyHeader):
    async def __call__(
        self, request: Request
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        api_key: str = request.headers.get(self.model.name)
        origin: Optional[str] = request.headers.get("origin")
        referrer: Optional[str] = request.headers.get("referer")  # !sic
        return _api_key_origin_auto_error(api_key, origin, referrer, self.auto_error)


async def get_api_key(
    api_key_query: Tuple[Optional[str], Optional[str], Optional[str]] = Security(
        APIKeyOriginQuery(name=API_KEY_NAME, auto_error=False)
    ),
    api_key_header: Tuple[Optional[str], Optional[str], Optional[str]] = Security(
        APIKeyOriginHeader(name=API_KEY_NAME, auto_error=False)
    ),
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    for api_key, origin, referrer in [api_key_query, api_key_header]:
        if api_key and (origin or referrer):
            try:
                row: ORMApiKey = await api_keys.get_api_key(UUID(api_key))
            except RecordNotFoundError:
                pass
            else:
                if api_key_is_valid(row.domains, row.expires_on, origin, referrer):
                    return api_key, origin, referrer

    raise HTTPException(
        status_code=HTTP_403_FORBIDDEN, detail="No valid API Key found."
    )


def api_key_is_valid(
    domains: List[str],
    expiration_date: Optional[datetime] = None,
    origin: Optional[str] = None,
    referrer: Optional[str] = None,
) -> bool:

    is_valid: bool = False

    # If there are any associate domains with the api key,
    # request needs to list a correct domain name in either origin or referrer header
    if not domains:
        is_valid = True
    elif origin and domains:
        is_valid = any([re.search(_to_regex(domain), origin) for domain in domains])
    elif referrer and domains:
        is_valid = any(
            [re.search(_to_regex(domain), _get_origin(referrer)) for domain in domains]
        )

    # The expiration date if any must be in the future
    if expiration_date and expiration_date < datetime.now():
        is_valid = False

    return is_valid


def _api_key_origin_auto_error(
    api_key: Optional[str],
    origin: Optional[str],
    referrer: Optional[str],
    auto_error: bool,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not api_key:
        if auto_error:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Not authenticated"
            )
        else:
            return None, origin, referrer
    return api_key, origin, referrer


def _to_regex(domain):
    result = domain.replace(".", r"\.").replace("*", ".*")
    return fr"^{result}$"


def _get_origin(referrer: str) -> str:
    return urlparse(referrer).netloc.split(":")[0]