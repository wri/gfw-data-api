import uuid
from datetime import datetime
from typing import List, Optional

from app.errors import RecordNotFoundError
from app.models.orm.api_keys import ApiKey as ORMApiKey

from ..utils.aws import get_api_gateway_client


async def create_api_key(
    user_id: str,
    alias: Optional[str],
    organization: str,
    email: str,
    domains: List[str],
    never_expires: bool,
) -> ORMApiKey:

    # If a simple string is used for domains, sqlalchemy will still inject this into db,
    # using every character as a list item. We don't want this to happen
    assert isinstance(domains, list), "Domains must be of type List[str]"

    new_api_key: ORMApiKey = await ORMApiKey.create(
        alias=alias,
        user_id=user_id,
        api_key=uuid.uuid4(),
        organization=organization,
        email=email,
        domains=domains,
        expires_on=None if never_expires else _next_year(),
    )

    return new_api_key


async def get_api_key(api_key: uuid.UUID) -> ORMApiKey:
    api_key_record: ORMApiKey = await ORMApiKey.get([api_key])
    if api_key_record is None:
        raise RecordNotFoundError(f"Could not find requested api_key {api_key}")

    return api_key_record


async def get_api_keys_from_user(user_id: str) -> List[ORMApiKey]:
    rows = await ORMApiKey.query.where(ORMApiKey.user_id == user_id).gino.all()

    return rows


async def delete_api_key(api_key: uuid.UUID) -> ORMApiKey:
    api_key_record: ORMApiKey = await get_api_key(api_key)
    await ORMApiKey.delete.where(ORMApiKey.api_key == api_key).gino.status()

    return api_key_record


async def add_api_key_to_gateway(
    name: str,
    key_value: str,
    rest_api_id: str,
    stage_name: str,
    usage_plan_id: str,
) -> dict:
    stage_keys = {
        "restApiId": rest_api_id,
        "stageName": stage_name,
    }
    gw_api_key = get_api_gateway_client().create_api_key(
        name=name,
        value=key_value,
        enabled=True,
        stageKeys=[stage_keys],
    )

    get_api_gateway_client().create_usage_plan_key(
        usagePlanId=usage_plan_id, keyId=gw_api_key["id"], keyType="API_KEY"
    )

    return gw_api_key


async def delete_api_key_from_gateway(name: str):
    response = get_api_gateway_client().get_api_keys(nameQuery=name)
    if len(response["items"]) == 0:
        raise RecordNotFoundError(f"API key with alias {name} not found in gateway")

    get_api_gateway_client().delete_api_key(apiKey=response["items"][0]["id"])


def _next_year(now=datetime.now()):
    """Return a date that's 1 year after the now.

    Return the same calendar date (month and day) in the destination
    year, if it exists, otherwise use the following day (thus changing
    February 29 to March 1).
    """
    try:
        return now.replace(year=now.year + 1)
    except ValueError:
        return now + (datetime(now.year + 1, 1, 1) - datetime(now.year, 1, 1))
