from uuid import UUID

from async_lru import alru_cache
from fastapi import HTTPException, Response
from fastapi.logger import logger
from httpx import AsyncClient, ReadTimeout
from httpx import Response as HTTPXResponse

from ..errors import (
    BadResponseError,
    InvalidResponseError,
    RecordNotFoundError,
    UnauthorizedError,
)
from ..models.pydantic.authentication import SignUp
from ..models.pydantic.geostore import Geometry, GeostoreCommon
from ..settings.globals import RW_API_URL


@alru_cache(maxsize=128)
async def get_geostore(geostore_id: UUID) -> GeostoreCommon:
    """Get RW Geostore geometry."""

    geostore_id_str: str = str(geostore_id).replace("-", "")

    url = f"{RW_API_URL}/v2/geostore/{geostore_id_str}"
    async with AsyncClient() as client:
        response: HTTPXResponse = await client.get(url)

    if response.status_code == 404:
        raise RecordNotFoundError(f"Geostore {geostore_id} not found")
    elif response.status_code != 200:
        logger.error(
            f"Response from RW API for geostore {geostore_id} was something "
            f"other than a 200 or 404. Status code: {response.status_code} "
            f"Response body: {response.text}"
        )
        raise InvalidResponseError("Call to Geostore failed")
    try:
        data = response.json()["data"]["attributes"]
        geojson = data["geojson"]["features"][0]["geometry"]
        geometry = Geometry(**geojson)
        geostore = GeostoreCommon(
            geostore_id=data["hash"],
            geojson=geometry,
            area__ha=data["areaHa"],
            bbox=data["bbox"],
        )
    except KeyError:
        logger.error(
            f"Response from RW API for geostore {geostore_id} contained "
            f"incomplete data. Response body: {response.text}"
        )
        raise BadResponseError("Cannot fetch geostore geometry")

    return geostore


async def who_am_i(token) -> Response:
    """Call GFW API to get token's identity."""

    headers = {"Authorization": f"Bearer {token}"}
    url = f"{RW_API_URL}/auth/check-logged"

    try:
        async with AsyncClient() as client:
            response: HTTPXResponse = await client.get(
                url, headers=headers, timeout=10.0
            )
    except ReadTimeout:
        raise HTTPException(
            status_code=500,
            detail="Call to authorization server timed-out. Please try again.",
        )

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

    logger.debug(
        f"Requesting Bearer token from GFW production API for user {user_name}"
    )

    url = f"{RW_API_URL}/auth/login"

    try:
        async with AsyncClient() as client:
            response: HTTPXResponse = await client.post(
                url, json=payload, headers=headers
            )
    except ReadTimeout:
        raise HTTPException(
            status_code=500,
            detail="Call to authorization server timed-out. Please try again.",
        )

    if response.status_code != 200:
        logger.warning(
            f"Authentication for user {user_name} failed. API responded with status code {response.status_code} and message {response.text}"
        )
        raise UnauthorizedError("Authentication failed")

    return response.json()["data"]["token"]


async def signup(name: str, email: str) -> SignUp:
    """Obtain a token form RW API using given user name and password."""

    headers = {"Content-Type": "application/json"}
    payload = {"name": name, "email": email, "apps": ["gfw"]}

    logger.debug(f"Create user account for user {name} with email {email}")

    url = f"{RW_API_URL}/auth/sign-up"

    try:
        async with AsyncClient() as client:
            response: HTTPXResponse = await client.post(
                url, json=payload, headers=headers
            )
    except ReadTimeout:
        raise HTTPException(
            status_code=500,
            detail="Call to authorization server timed-out. Please try again.",
        )

    if response.status_code == 422:
        raise HTTPException(
            status_code=422,
            detail="An account already exists for the provided email address.",
        )

    elif response.status_code != 200:
        logger.error(
            "An error occurred while trying to create a new user account",
            response.json(),
        )
        raise HTTPException(
            status_code=500,
            detail="An error occurred while trying to create a new user account. Please try again.",
        )

    return SignUp(**response.json()["data"])
