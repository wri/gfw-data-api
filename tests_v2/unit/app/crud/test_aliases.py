import pytest

from app.application import ContextEngine
from app.crud.aliases import create_alias, delete_alias, get_alias
from app.errors import RecordNotFoundError
from app.models.orm.aliases import Alias as ORMAlias


@pytest.mark.asyncio
async def test_alias(generic_vector_source_version):
    dataset, version, _ = generic_vector_source_version
    alias = "v202103"

    # test create
    async with ContextEngine("WRITE"):
        row: ORMAlias = await create_alias(alias, dataset, version)

    assert row.alias == alias
    assert row.dataset == dataset
    assert row.version == version

    # test get
    async with ContextEngine("READ"):
        row: ORMAlias = await get_alias(dataset, alias)

    assert row.alias == alias
    assert row.dataset == dataset
    assert row.version == version

    # test delete
    async with ContextEngine("WRITE"):
        row: ORMAlias = await delete_alias(dataset, alias)

    with pytest.raises(RecordNotFoundError):
        await get_alias(dataset, alias)
