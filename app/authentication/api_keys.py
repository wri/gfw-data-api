import re
from datetime import datetime
from typing import Optional, Tuple
from uuid import UUID

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyCookie, APIKeyHeader, APIKeyQuery
from starlette.status import HTTP_403_FORBIDDEN

from ..crud import api_keys
from ..errors import RecordNotFoundError
from ..models.orm.api_keys import ApiKey as ORMApiKey
from ..settings.globals import API_KEY_NAME


class APIKeyOriginQuery(APIKeyQuery):
    async def __call__(self, request: Request) -> Tuple[Optional[str], Optional[str]]:
        api_key: str = request.query_params.get(self.model.name)
        origin: Optional[str] = request.headers.get("origin")
        return _api_key_origin_auto_error(api_key, origin, self.auto_error)


class APIKeyOriginHeader(APIKeyHeader):
    async def __call__(self, request: Request) -> Tuple[Optional[str], Optional[str]]:
        api_key: str = request.headers.get(self.model.name)
        origin: Optional[str] = request.headers.get("origin")
        return _api_key_origin_auto_error(api_key, origin, self.auto_error)


class APIKeyOriginCookie(APIKeyCookie):
    async def __call__(self, request: Request) -> Tuple[Optional[str], Optional[str]]:
        api_key = request.cookies.get(self.model.name)
        origin: Optional[str] = request.headers.get("origin")
        return _api_key_origin_auto_error(api_key, origin, self.auto_error)


async def get_api_key(
    api_key_query: Tuple[Optional[str], Optional[str]] = Security(
        APIKeyOriginQuery(name=API_KEY_NAME, auto_error=False)
    ),
    api_key_header: Tuple[Optional[str], Optional[str]] = Security(
        APIKeyOriginHeader(name=API_KEY_NAME, auto_error=False)
    ),
    api_key_cookie: Tuple[Optional[str], Optional[str]] = Security(
        APIKeyOriginCookie(name=API_KEY_NAME, auto_error=False)
    ),
):
    for api_key, origin in [api_key_query, api_key_header, api_key_cookie]:
        if api_key and origin:
            try:
                row: ORMApiKey = await api_keys.get_api_key(UUID(api_key))
            except RecordNotFoundError:
                pass
            else:
                if (
                    any(
                        [re.search(_to_regex(domain), origin) for domain in row.domains]
                    )
                    and row.expiration_date >= datetime.now()
                ):
                    return api_key, origin

    raise HTTPException(
        status_code=HTTP_403_FORBIDDEN, detail="No valid API Key found."
    )


def _api_key_origin_auto_error(
    api_key: Optional[str], origin: Optional[str], auto_error: bool
) -> Tuple[Optional[str], Optional[str]]:
    if not api_key:
        if auto_error:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Not authenticated"
            )
        else:
            return None, origin
    return api_key, origin


def _to_regex(domain):
    result = domain.replace(".", r"\.").replace("*", ".*")
    return fr"^{result}$"
