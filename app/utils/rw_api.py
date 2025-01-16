from typing import Dict
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
from ..models.pydantic.authentication import User
from ..models.pydantic.geostore import (
    Geometry,
    GeostoreCommon,
    RWAdminListResponse,
    RWCalcAreaForGeostoreResponse,
    RWGeostoreResponse,
    RWViewGeostoreResponse,
)
from ..settings.globals import RW_API_URL, SERVICE_ACCOUNT_TOKEN


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
            f"Authentication for user {user_name} failed. API responded with status code {response.status_code} and message {response.text}"
        )
        raise UnauthorizedError("Authentication failed")

    return response.json()["data"]["token"]


async def signup(name: str, email: str) -> User:
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

    return User(**response.json()["data"])


async def calc_area(
    payload: Dict, x_api_key: str | None = None
) -> RWCalcAreaForGeostoreResponse:
    url = f"{RW_API_URL}/v1/geostore/area"

    async with AsyncClient() as client:
        if x_api_key is not None:
            response: HTTPXResponse = await client.post(
                url, json=payload, headers={"x-api-key": x_api_key}
            )
        else:
            response = await client.get(url)

    if response.status_code == 200:
        return RWCalcAreaForGeostoreResponse.parse_obj(response.json())
    else:
        raise HTTPException(response.status_code, response.text)


async def find_by_ids(payload: Dict) -> HTTPXResponse:
    url = f"{RW_API_URL}/v2/geostore/find_by_ids"

    async with AsyncClient() as client:
        response: HTTPXResponse = await client.post(url, json=payload)
    return response


async def get_admin_list(x_api_key: str | None = None) -> RWAdminListResponse:
    url = f"{RW_API_URL}/v2/geostore/admin/list"

    async with AsyncClient() as client:
        if x_api_key is not None:
            response: HTTPXResponse = await client.get(
                url, headers={"x-api-key": x_api_key}
            )
        else:
            response = await client.get(url)

    if response.status_code == 200:
        return RWAdminListResponse.parse_obj(response.json())
    else:
        raise HTTPException(response.status_code, response.text)


async def get_boundary_by_country_id(
    country_id: str, x_api_key: str | None = None
) -> RWGeostoreResponse:
    url = f"{RW_API_URL}/v2/geostore/admin/{country_id}"

    async with AsyncClient() as client:
        if x_api_key is not None:
            response: HTTPXResponse = await client.get(
                url, headers={"x-api-key": x_api_key}
            )
        else:
            response = await client.get(url)

    if response.status_code == 200:
        return RWGeostoreResponse.parse_obj(response.json())
    else:
        raise HTTPException(response.status_code, response.text)


async def get_boundary_by_region_id(
    country_id: str, region_id: str, x_api_key: str | None = None
) -> RWGeostoreResponse:
    url = f"{RW_API_URL}/v2/geostore/admin/{country_id}/{region_id}"

    async with AsyncClient() as client:
        if x_api_key is not None:
            response: HTTPXResponse = await client.get(
                url, headers={"x-api-key": x_api_key}
            )
        else:
            response = await client.get(url)

    if response.status_code == 200:
        return RWGeostoreResponse.parse_obj(response.json())
    else:
        raise HTTPException(response.status_code, response.text)


async def get_boundary_by_subregion_id(
    country_id: str, region_id: str, subregion_id: str, x_api_key: str | None = None
) -> RWGeostoreResponse:
    url = f"{RW_API_URL}/v2/geostore/admin/{country_id}/{region_id}/{subregion_id}"

    async with AsyncClient() as client:
        if x_api_key is not None:
            response: HTTPXResponse = await client.get(
                url, headers={"x-api-key": x_api_key}
            )
        else:
            response = await client.get(url)

    if response.status_code == 200:
        return RWGeostoreResponse.parse_obj(response.json())
    else:
        raise HTTPException(response.status_code, response.text)


async def get_geostore_by_land_use_and_index(
    land_use_type: str, index: str, x_api_key: str | None = None
) -> RWGeostoreResponse:
    url = f"{RW_API_URL}/v2/geostore/use/{land_use_type}/{index}"

    async with AsyncClient() as client:
        if x_api_key is not None:
            response: HTTPXResponse = await client.get(
                url, headers={"x-api-key": x_api_key}
            )
        else:
            response = await client.get(url)

    if response.status_code == 200:
        return RWGeostoreResponse.parse_obj(response.json())
    else:
        raise HTTPException(response.status_code, response.text)


async def get_geostore_by_wdpa_id(
    wdpa_id: str, x_api_key: str | None = None
) -> RWGeostoreResponse:
    url = f"{RW_API_URL}/v2/geostore/wdpa/{wdpa_id}"

    async with AsyncClient() as client:
        if x_api_key is not None:
            response: HTTPXResponse = await client.get(
                url, headers={"x-api-key": x_api_key}
            )
        else:
            response = await client.get(url)

    if response.status_code == 200:
        return RWGeostoreResponse.parse_obj(response.json())
    else:
        raise HTTPException(response.status_code, response.text)


async def get_view_geostore_by_id(
    rw_geostore_id: str, x_api_key: str | None = None
) -> RWViewGeostoreResponse:
    url = f"{RW_API_URL}/v2/geostore/{rw_geostore_id}/view"

    async with AsyncClient() as client:
        if x_api_key is not None:
            response: HTTPXResponse = await client.get(
                url, headers={"x-api-key": x_api_key}
            )
        else:
            response = await client.get(url)

    if response.status_code == 200:
        return RWViewGeostoreResponse.parse_obj(response.json())
    else:
        raise HTTPException(response.status_code, response.text)
