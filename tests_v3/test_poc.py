# from datetime import datetime, timedelta
# from uuid import UUID
from datetime import timedelta, datetime
from uuid import UUID

import pytest
import pytest_asyncio

from app.application import ContextEngine
from app.crud.api_keys import (
    create_api_key,
)
from app.crud.assets import get_default_asset
from app.crud.datasets import get_dataset, update_dataset, create_dataset
from app.crud.versions import get_version, create_version, get_latest_version
from app.models.orm.api_keys import ApiKey as ORMApiKey


GOOD_DOMAINS = [
    "www.globalforestwatch.org",
    "*.globalforestwatch.org",
    "globalforestwatch.org",
    "localhost",
]

GOOD_PAYLOAD = (
    "abc",
    "my_nickname",
    "my_organization",
    "my.email@test.com",
    GOOD_DOMAINS,
)


# @pytest_asyncio.fixture(autouse=True)
# @pytest.mark.asyncio
# async def delete_api_keys(app, db_ready):
#     yield
#     async with ContextEngine("WRITE"):
#         await ORMApiKey.delete.gino.status()
#

# @pytest.mark.parametrize("user_id, alias, organization, email, domains", [GOOD_PAYLOAD])
# @pytest.mark.asyncio
# async def test_create_api_key(app, db_ready, user_id, alias, organization, email, domains):
#     async with ContextEngine("WRITE"):
#         row: ORMApiKey = await create_api_key(
#             user_id, alias, organization, email, domains, never_expires=False
#         )
#
#     assert isinstance(row.api_key, UUID)
#     assert row.user_id == user_id
#     assert row.alias == alias
#     assert row.organization == organization
#     assert row.email == email
#     assert row.domains == domains
#     assert (row.expires_on - datetime.now()).total_seconds() == pytest.approx(
#         timedelta(days=365).total_seconds(), 0.1
#     )
#     assert isinstance(row.created_on, datetime)
#     assert isinstance(row.updated_on, datetime)
#
#
# @pytest.mark.asyncio
# async def test_foo(app, db_ready):
#     assert 1 == 1


@pytest.mark.asyncio
async def test_latest_versions(app, db_ready):
    """Test if trigger function on versions table work It is suppose to reset
    is_latest field to False for all versions of a dataset Once a version's
    is_latest field is set to True Get Latest Version function should always
    return the latest version number."""

    dataset_name = "test"

    # Add a dataset
    async with ContextEngine("WRITE"):
        await create_dataset(dataset_name)
        await create_version(dataset_name, "v1.1.1", is_latest=True)
        await create_version(dataset_name, "v1.1.2", is_latest=True)
        latest = await get_latest_version(dataset_name)
        first_row = await get_version(dataset_name, "v1.1.1")
        second_row = await get_version(dataset_name, "v1.1.2")

    assert first_row.is_latest is False
    assert second_row.is_latest is True
    assert latest == "v1.1.2"
