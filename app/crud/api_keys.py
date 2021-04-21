import uuid
from datetime import datetime
from typing import List

from app.errors import RecordNotFoundError
from app.models.orm.api_keys import ApiKey as ORMApiKey


async def create_api_key(
    organization: str, email: str, domains: List[str]
) -> ORMApiKey:

    new_api_key: ORMApiKey = await ORMApiKey.create(
        api_key=uuid.uuid4(),
        organization=organization,
        email=email,
        domains=domains,
        expiration_date=_next_year(),
    )

    return new_api_key


async def get_api_key(api_key: uuid.UUID) -> ORMApiKey:
    api_key_record: ORMApiKey = await ORMApiKey.get([api_key])
    if api_key_record is None:
        raise RecordNotFoundError(f"Could not find requested api_key {api_key}")

    return api_key_record


def _next_year():
    """Return a date that's 1 year after the now.

    Return the same calendar date (month and day) in the destination
    year, if it exists, otherwise use the following day (thus changing
    February 29 to March 1).
    """
    now = datetime.now()
    try:
        return now.replace(year=now.year + 1)
    except ValueError:
        return now + (datetime(now.year + 1, 1, 1) - datetime(now.year, 1, 1))
