from typing import Dict
from uuid import UUID

from async_lru import alru_cache
from fastapi import HTTPException, Response
from fastapi.logger import logger
from httpx import AsyncClient, ReadTimeout
from httpx import Response as HTTPXResponse
from starlette.requests import QueryParams

from ..errors import (
    BadResponseError,
    InvalidResponseError,
    RecordNotFoundError,
    UnauthorizedError,
)
from ..models.pydantic.authentication import User
from ..models.pydantic.geostore import (
    AdminGeostoreResponse,
    AdminListResponse,
    Geometry,
    GeostoreCommon,
    RWGeostoreIn,
)
from ..settings.globals import RW_API_KEY, RW_API_URL, SERVICE_ACCOUNT_TOKEN


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


async def get_rw_user(user_id: str) -> User:
    """Call RW API to get a user from RW API."""

    headers = {"Authorization": f"Bearer {SERVICE_ACCOUNT_TOKEN}"}
    url = f"{RW_API_URL}/auth/user/{user_id}"

    try:
        async with AsyncClient() as client:
            response: HTTPXResponse = await client.get(
                url, headers=headers, timeout=10.0
            )
    except ReadTimeout:
        raise HTTPException(
            status_code=500,
            detail="Call to user service timed-out. Please try again.",
        )

    if response.status_code == 404:
        raise HTTPException(status_code=401, detail=f"User ID invalid: {user_id}")

    if response.status_code != 200:
        logger.warning(
            f"Failed to authorize user. Server responded with response code: {response.status_code} and message: {response.text}"
        )
        raise HTTPException(
            status_code=500, detail="Call to user service server failed"
        )

    return User(**response.json())


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
            f"Authentication for user {user_name} failed. "
            f"API responded with status code {response.status_code} "
            f"and message {response.text}"
        )
        raise UnauthorizedError("Authentication failed")

    return response.json()["data"]["token"]


async def signup(name: str, email: str) -> User:
    """Obtain a token from RW API using given username and password."""

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

    return User(**response.json()["data"])


async def create_rw_geostore(payload: RWGeostoreIn) -> AdminGeostoreResponse:
    url = f"{RW_API_URL}/v1/geostore"

    async with AsyncClient() as client:
        response: HTTPXResponse = await client.post(
            url, json=payload.dict(), headers={"x-api-key": RW_API_KEY}
        )

    if response.status_code == 200:
        return AdminGeostoreResponse.parse_obj(response.json())
    else:
        raise HTTPException(response.status_code, response.text)


async def proxy_get_geostore(
    geostore_id: str, query_params: QueryParams
) -> AdminGeostoreResponse:
    url = f"{RW_API_URL}/v2/geostore/{geostore_id}"

    response = await proxy_get_request_to_rw_api(url, dict(**query_params))
    return AdminGeostoreResponse.parse_obj(response.json())


async def rw_get_admin_list(query_params: QueryParams) -> AdminListResponse:
    url = f"{RW_API_URL}/v2/geostore/admin/list"

    response = await proxy_get_request_to_rw_api(url, dict(**query_params))
    return AdminListResponse.parse_obj(response.json())


async def get_boundary_by_country_id(
    country_id: str, query_params: QueryParams
) -> AdminGeostoreResponse:
    url = f"{RW_API_URL}/v2/geostore/admin/{country_id}"

    response = await proxy_get_request_to_rw_api(url, dict(**query_params))
    return AdminGeostoreResponse.parse_obj(response.json())


async def get_boundary_by_region_id(
    country_id: str,
    region_id: str,
    query_params: QueryParams,
) -> AdminGeostoreResponse:
    url = f"{RW_API_URL}/v2/geostore/admin/{country_id}/{region_id}"

    response = await proxy_get_request_to_rw_api(url, dict(**query_params))
    return AdminGeostoreResponse.parse_obj(response.json())


async def get_boundary_by_subregion_id(
    country_id: str,
    region_id: str,
    subregion_id: str,
    query_params: QueryParams,
) -> AdminGeostoreResponse:
    url = f"{RW_API_URL}/v2/geostore/admin/{country_id}/{region_id}/{subregion_id}"

    response = await proxy_get_request_to_rw_api(url, dict(**query_params))
    return AdminGeostoreResponse.parse_obj(response.json())


async def get_geostore_by_land_use_and_index(
    land_use_type: str,
    index: str,
    query_params: QueryParams,
) -> AdminGeostoreResponse:
    url = f"{RW_API_URL}/v2/geostore/use/{land_use_type}/{index}"

    response = await proxy_get_request_to_rw_api(url, dict(**query_params))
    return AdminGeostoreResponse.parse_obj(response.json())


async def proxy_get_request_to_rw_api(url: str, query_params: Dict) -> HTTPXResponse:
    headers = {}
    if RW_API_KEY is not None:
        headers["x-api-key"] = RW_API_KEY

    async with AsyncClient() as client:
        response: HTTPXResponse = await client.get(
            url, headers=headers, params=query_params
        )

    if response.status_code == 200:
        return response
    else:
        raise HTTPException(response.status_code, response.text)
