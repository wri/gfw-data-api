import pytest

from app.application import ContextEngine
from app.models.orm.api_keys import ApiKey as ORMApiKey


@pytest.fixture(scope="module", autouse=True)
def crud_module_db(module_db):
    """auto use module db."""
    yield


@pytest.fixture(autouse=True)
def crud_init_db(init_db):
    """auto use init db."""
    yield


@pytest.fixture(autouse=True)
@pytest.mark.asyncio
async def delete_api_keys():
    yield
    async with ContextEngine("WRITE"):
        await ORMApiKey.delete.gino.status()
