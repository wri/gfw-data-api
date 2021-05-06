import uuid
from datetime import datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from app.application import ContextEngine
from app.crud.api_keys import (
    _next_year,
    create_api_key,
    delete_api_key,
    get_api_key,
    get_api_keys_from_user,
)
from app.errors import RecordNotFoundError
from app.models.orm.api_keys import ApiKey as ORMApiKey
from tests_v2.fixtures.authentication.api_keys import GOOD_DOMAINS

GOOD_PAYLOAD = (
    "abc",
    "my_nickname",
    "my_organization",
    "my.email@test.com",
    GOOD_DOMAINS,
)


@pytest.mark.parametrize("user_id, alias, organization, email, domains", [GOOD_PAYLOAD])
@pytest.mark.asyncio
async def test_create_api_key(user_id, alias, organization, email, domains):
    async with ContextEngine("WRITE"):
        row: ORMApiKey = await create_api_key(
            user_id, alias, organization, email, domains
        )

    assert isinstance(row.api_key, UUID)
    assert row.user_id == user_id
    assert row.alias == alias
    assert row.organization == organization
    assert row.email == email
    assert row.domains == domains
    assert (row.expires_on - datetime.now()).total_seconds() == pytest.approx(
        timedelta(days=365).total_seconds(), 0.1
    )
    assert isinstance(row.created_on, datetime)
    assert isinstance(row.updated_on, datetime)


@pytest.mark.parametrize("user_id, alias, organization, email, domains", [GOOD_PAYLOAD])
@pytest.mark.asyncio
async def test_create_api_key_unique_constraint(
    user_id, alias, organization, email, domains
):
    """We should be able to submit the same payload twice."""
    async with ContextEngine("WRITE"):
        await create_api_key(user_id, alias, organization, email, domains)
        with pytest.raises(asyncpg.exceptions.UniqueViolationError):
            await create_api_key(user_id, alias, organization, email, domains)


@pytest.mark.parametrize(
    "user_id, alias, organization, email, domains",
    [
        (None, "my_nickname", "my_organization", "my.email@test.com", GOOD_DOMAINS),
        ("abc", None, "my_organization", "my.email@test.com", GOOD_DOMAINS),
        ("abc", "my_nickname", None, "my.email@test.com", GOOD_DOMAINS),
        ("abc", "my_nickname", "my_organization", None, GOOD_DOMAINS),
    ],
)
@pytest.mark.asyncio
async def test_create_api_key_missing_value(
    user_id, alias, organization, email, domains
):
    async with ContextEngine("WRITE"):
        with pytest.raises(asyncpg.exceptions.NotNullViolationError):
            await create_api_key(user_id, alias, organization, email, domains)


@pytest.mark.parametrize(
    "user_id, alias, organization, email, domains",
    [
        (1, "my_nickname", "my_organization", "my.email@test.com", GOOD_DOMAINS),
        ([], "my_nickname", "my_organization", "my.email@test.com", GOOD_DOMAINS),
        ("abc", 1, "my_organization", "my.email@test.com", GOOD_DOMAINS),
        ("abc", [], "my_organization", "my.email@test.com", GOOD_DOMAINS),
        ("abc", "my_nickname", 1, "my.email@test.com", GOOD_DOMAINS),
        ("abc", "my_nickname", [], "my.email@test.com", GOOD_DOMAINS),
        ("abc", "my_nickname", "my_organization", 1, GOOD_DOMAINS),
        ("abc", "my_nickname", "my_organization", [], GOOD_DOMAINS),
    ],
)
@pytest.mark.asyncio
async def test_create_api_key_wrong_type(user_id, alias, organization, email, domains):
    async with ContextEngine("WRITE"):
        with pytest.raises(asyncpg.exceptions.DataError):
            await create_api_key(user_id, alias, organization, email, domains)


@pytest.mark.parametrize(
    "user_id, alias, organization, email, domains",
    [
        ("abc", "my_nickname", "my_organization", "my.email@test.com", None),
        ("abc", "my_nickname", "my_organization", "my.email@test.com", 1),
        ("abc", "my_nickname", "my_organization", "my.email@test.com", "my_domain"),
    ],
)
@pytest.mark.asyncio
async def test_create_api_key_domains_not_list(
    user_id, alias, organization, email, domains
):
    async with ContextEngine("WRITE"):
        with pytest.raises(AssertionError):
            await create_api_key(user_id, alias, organization, email, domains)


@pytest.mark.parametrize("user_id, alias, organization, email, domains", [GOOD_PAYLOAD])
@pytest.mark.asyncio
async def test_get_api_key(user_id, alias, organization, email, domains):
    async with ContextEngine("WRITE"):
        create_row: ORMApiKey = await create_api_key(
            user_id, alias, organization, email, domains
        )
        get_row: ORMApiKey = await get_api_key(create_row.api_key)

    assert create_row.api_key == get_row.api_key
    assert create_row.alias == get_row.alias
    assert create_row.email == get_row.email
    assert create_row.organization == get_row.organization
    assert create_row.expires_on == get_row.expires_on
    assert create_row.created_on == get_row.created_on
    assert create_row.updated_on == get_row.updated_on


@pytest.mark.parametrize("api_key", [uuid.uuid4(), uuid.uuid4(), None])
@pytest.mark.asyncio
async def test_get_api_key_bad_key(api_key):
    async with ContextEngine("READ"):
        with pytest.raises(RecordNotFoundError):
            await get_api_key(api_key)


@pytest.mark.parametrize("api_key", ["random_string", 1])
@pytest.mark.asyncio
async def test_get_api_key_bad_type(api_key):
    async with ContextEngine("READ"):
        with pytest.raises(asyncpg.exceptions.DataError):
            await get_api_key(api_key)


@pytest.mark.parametrize("user_id, alias, organization, email, domains", [GOOD_PAYLOAD])
@pytest.mark.asyncio
async def test_get_api_key_from_user(user_id, alias, organization, email, domains):

    api_keys1 = list()
    api_keys2 = list()
    async with ContextEngine("WRITE"):

        row: ORMApiKey = await create_api_key(
            user_id, alias, organization, email, domains
        )
        api_keys1.append(row.api_key)

        row = await create_api_key(
            user_id, str(uuid.uuid4()), organization, email, domains
        )
        api_keys1.append(row.api_key)

        new_user_id = str(uuid.uuid4())
        row = await create_api_key(new_user_id, alias, organization, email, domains)
        api_keys2.append(row.api_key)

        rows = await get_api_keys_from_user(user_id)
        assert len(rows) == len(api_keys1)
        for row in rows:
            assert row.api_key in api_keys1

        rows = await get_api_keys_from_user(new_user_id)
        assert len(rows) == len(api_keys2)
        for row in rows:
            assert row.api_key in api_keys2


@pytest.mark.parametrize("user_id, alias, organization, email, domains", [GOOD_PAYLOAD])
@pytest.mark.asyncio
async def test_delete_api_key(user_id, alias, organization, email, domains):
    async with ContextEngine("WRITE"):
        create_row: ORMApiKey = await create_api_key(
            user_id, alias, organization, email, domains
        )

        delete_row: ORMApiKey = await delete_api_key(create_row.api_key)

        with pytest.raises(RecordNotFoundError):
            await get_api_key(create_row.api_key)

        assert create_row.api_key == delete_row.api_key
        assert create_row.alias == delete_row.alias
        assert create_row.email == delete_row.email
        assert create_row.organization == delete_row.organization
        assert create_row.expires_on == delete_row.expires_on
        assert create_row.created_on == delete_row.created_on
        assert create_row.updated_on == delete_row.updated_on


@pytest.mark.parametrize(
    "input_date, expected_result",
    [
        (datetime(year=2021, month=4, day=30), datetime(year=2022, month=4, day=30)),
        (datetime(year=2021, month=12, day=31), datetime(year=2022, month=12, day=31)),
        (datetime(year=2020, month=2, day=29), datetime(year=2021, month=3, day=1)),
    ],
)
@pytest.mark.asyncio
async def test__next_year(
    input_date, expected_result
):  # needs to be async due to auto use fixtures
    assert _next_year(input_date) == expected_result
