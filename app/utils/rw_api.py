from uuid import UUID

from async_lru import alru_cache
from fastapi import HTTPException, Response
from fastapi.logger import logger
from httpx import AsyncClient
from httpx import Response as HTTPXResponse

from ..errors import BadResponseError, InvalidResponseError, UnauthorizedError
from ..models.pydantic.geostore import Geometry
from ..settings.globals import ENV


@alru_cache(maxsize=128)
async def get_geostore_geometry(geostore_id: UUID) -> Geometry:
    """Get RW Geostore geometry."""

    prefix = _env_prefix()
    geostore_id_str: str = str(geostore_id).replace("-", "")

    url = f"https://{prefix}-api.globalforestwatch.org/v2/geostore/{geostore_id_str}"
    async with AsyncClient() as client:
        response: HTTPXResponse = await client.get(url)

    if response.status_code != 200:
        raise InvalidResponseError("Call to Geostore failed")
    try:
        geometry = response.json()["data"]["attributes"]["geojson"]["features"][0][
            "geometry"
        ]
    except KeyError:
        raise BadResponseError("Cannot fetch geostore geometry")

    return geometry


async def who_am_i(token) -> Response:
    """Call GFW API to get token's identity."""

    prefix = _env_prefix()

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://{prefix}-api.globalforestwatch.org/auth/check-logged"

    async with AsyncClient() as client:
        response: HTTPXResponse = await client.get(url, headers=headers)

    if response.status_code != 200 and response.status_code != 401:
        logger.warning(
            f"Failed to authorize user. Server responded with response code: {response.status_code} and message: {response.text}"
        )
        raise HTTPException(
            status_code=500, detail="Call to authorization server failed"
        )

    return response


async def login(user_name: str, password: str) -> str:
    """Obtain a token form RW API using given user name and password."""

    headers = {"Content-Type": "application/json"}
    payload = {"email": user_name, "password": password}

    prefix = _env_prefix()

    logger.debug(
        f"Requesting Bearer token from GFW production API for user {user_name}"
    )

    url = f"https://{prefix}-api.globalforestwatch.org/auth/login"

    async with AsyncClient() as client:
        response: HTTPXResponse = await client.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        logger.warning(
            f"Authentication for user {user_name} failed. API responded with status code {response.status_code} and message {response.text}"
        )
        raise UnauthorizedError("Authentication failed")

    return response.json()["data"]["token"]


def _env_prefix() -> str:
    """Set RW environment."""
    if ENV in ("dev", "test"):
        prefix = "staging"
    else:
        prefix = ENV

    return prefix
