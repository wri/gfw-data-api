import pytest

from app.application import ContextEngine
from app.crud.assets import get_default_asset
from app.crud.datasets import get_dataset
from app.crud.versions import get_version, update_version


@pytest.mark.asyncio
async def test_update_version__is_downloadable(generic_vector_source_version):
    dataset, version, _ = generic_vector_source_version
    dataset_row = await get_dataset(dataset)
    version_row = await get_version(dataset, version)
    asset_row = await get_default_asset(dataset, version)

    # Check if default value is correctly populated
    assert dataset_row.is_downloadable is True
    assert version_row.is_downloadable is True
    assert asset_row.is_downloadable is True

    # This should update the downstream versions and assets only
    async with ContextEngine("WRITE"):
        await update_version(dataset, version, **{"is_downloadable": False})

    dataset_row = await get_dataset(dataset)
    version_row = await get_version(dataset, version)
    asset_row = await get_default_asset(dataset, version)

    assert dataset_row.is_downloadable is True
    assert version_row.is_downloadable is False
    assert asset_row.is_downloadable is False
