import uuid
from datetime import datetime, timedelta
from uuid import UUID

import asyncpg
import boto3
import pytest
import pytest_asyncio
from moto import mock_apigateway

from app.application import ContextEngine
from app.crud.api_keys import (
    _next_year,
    add_api_key_to_gateway,
    create_api_key,
    delete_api_key,
    delete_api_key_from_gateway,
    get_api_key,
    get_api_keys_from_user,
)
from app.errors import RecordNotFoundError
from app.models.orm.api_keys import ApiKey as ORMApiKey
from app.settings.globals import API_GATEWAY_STAGE_NAME
from tests_v2.fixtures.authentication.api_keys import GOOD_DOMAINS

GOOD_PAYLOAD = (
    "abc",
    "my_nickname",
    "my_organization",
    "my.email@test.com",
    GOOD_DOMAINS,
)


@pytest_asyncio.fixture(autouse=True)
@pytest.mark.asyncio
async def delete_api_keys():
    yield
    async with ContextEngine("WRITE"):
        await ORMApiKey.delete.gino.status()


@pytest.mark.parametrize("user_id, alias, organization, email, domains", [GOOD_PAYLOAD])
@pytest.mark.asyncio
async def test_create_api_key(user_id, alias, organization, email, domains):
    async with ContextEngine("WRITE"):
        row: ORMApiKey = await create_api_key(
            user_id, alias, organization, email, domains, never_expires=False
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
async def test_create_api_key_never_expires(
    user_id, alias, organization, email, domains
):
    async with ContextEngine("WRITE"):
        row: ORMApiKey = await create_api_key(
            user_id, alias, organization, email, domains, never_expires=True
        )

    assert isinstance(row.api_key, UUID)
    assert row.user_id == user_id
    assert row.alias == alias
    assert row.organization == organization
    assert row.email == email
    assert row.domains == domains
    assert row.expires_on is None
    assert isinstance(row.created_on, datetime)
    assert isinstance(row.updated_on, datetime)


@pytest.mark.parametrize("user_id, alias, organization, email, domains", [GOOD_PAYLOAD])
@pytest.mark.asyncio
async def test_create_api_key_empty_domains(
    user_id, alias, organization, email, domains
):
    async with ContextEngine("WRITE"):
        row: ORMApiKey = await create_api_key(
            user_id, alias, organization, email, domains=[], never_expires=True
        )

    assert isinstance(row.api_key, UUID)
    assert row.user_id == user_id
    assert row.alias == alias
    assert row.organization == organization
    assert row.email == email
    assert row.domains == []
    assert row.expires_on is None
    assert isinstance(row.created_on, datetime)
    assert isinstance(row.updated_on, datetime)


@pytest.mark.parametrize("user_id, alias, organization, email, domains", [GOOD_PAYLOAD])
@pytest.mark.asyncio
async def test_create_api_key_unique_constraint(
    user_id, alias, organization, email, domains
):
    """We should be able to submit the same payload twice."""
    async with ContextEngine("WRITE"):
        await create_api_key(
            user_id, alias, organization, email, domains, never_expires=False
        )
        with pytest.raises(asyncpg.exceptions.UniqueViolationError):
            await create_api_key(
                user_id, alias, organization, email, domains, never_expires=False
            )


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
            await create_api_key(
                user_id, alias, organization, email, domains, never_expires=False
            )


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
            await create_api_key(
                user_id, alias, organization, email, domains, never_expires=False
            )


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
            await create_api_key(
                user_id, alias, organization, email, domains, never_expires=False
            )


@pytest.mark.parametrize("user_id, alias, organization, email, domains", [GOOD_PAYLOAD])
@pytest.mark.asyncio
async def test_get_api_key(user_id, alias, organization, email, domains):
    async with ContextEngine("WRITE"):
        create_row: ORMApiKey = await create_api_key(
            user_id, alias, organization, email, domains, never_expires=False
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
            user_id, alias, organization, email, domains, never_expires=False
        )
        api_keys1.append(row.api_key)

        row = await create_api_key(
            user_id,
            str(uuid.uuid4()),
            organization,
            email,
            domains,
            never_expires=False,
        )
        api_keys1.append(row.api_key)

        new_user_id = str(uuid.uuid4())
        row = await create_api_key(
            new_user_id, alias, organization, email, domains, never_expires=False
        )
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
            user_id, alias, organization, email, domains, never_expires=False
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


@pytest.mark.asyncio
async def test_add_api_key_to_gateway():
    with mock_apigateway():
        test_key = "test_value_greater_than_20_chars"
        client = boto3.client("apigateway", region_name="us-east-1")
        rest_api = client.create_rest_api(name="test")
        root_resource = client.get_resources(restApiId=rest_api["id"])["items"][0]
        client.put_method(
            restApiId=rest_api["id"],
            resourceId=root_resource["id"],
            httpMethod="GET",
            authorizationType="none",
            apiKeyRequired=True,
        )
        client.put_integration(
            restApiId=rest_api["id"],
            resourceId=root_resource["id"],
            httpMethod="GET",
            type="HTTP",
            uri="http://httpbin.org/robots.txt",
            integrationHttpMethod="POST",
        )
        deployment = client.create_deployment(
            restApiId=rest_api["id"], description="test"
        )

        client.create_stage(
            restApiId=rest_api["id"],
            stageName=API_GATEWAY_STAGE_NAME,
            deploymentId=deployment["id"],
        )
        api_stages = [
            {"apiId": rest_api["id"], "stage": API_GATEWAY_STAGE_NAME},
        ]
        throttle = {"burstLimit": 123, "rateLimit": 123.0}
        usage_plan = client.create_usage_plan(
            name="internal", apiStages=api_stages, throttle=throttle
        )

        gw_key = await add_api_key_to_gateway(
            "test_name",
            test_key,
            rest_api["id"],
            API_GATEWAY_STAGE_NAME,
            usage_plan["id"],
        )
        usage_plan_key = client.get_usage_plan_key(
            usagePlanId=usage_plan["id"], keyId=gw_key["id"]
        )

        assert gw_key["value"] == test_key
        assert usage_plan_key["value"] == test_key


@pytest.mark.asyncio
async def test_delete_api_key_from_gateway():
    with mock_apigateway():
        client = boto3.client("apigateway", region_name="us-east-1")
        gw_key = client.create_api_key(
            name="key_name", value="test_value_greater_than_20_chars", enabled=True
        )

        await delete_api_key_from_gateway(gw_key["name"])
        response = client.get_api_keys(nameQuery=gw_key["name"])

        assert len(response["items"]) == 0


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
