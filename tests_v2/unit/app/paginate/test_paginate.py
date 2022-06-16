from unittest.mock import Mock

import pytest

from app.crud.datasets import get_datasets
from app.models.orm.datasets import Dataset as ORMDataset
from app.paginate.paginate import paginate_datasets


@pytest.mark.asyncio
async def test_legacy_no_pagination_happens_for_default_values():
    """This is for legacy compatibility"""
    spy_get_datasets = Mock(get_datasets)

    await paginate_datasets(crud_impl=spy_get_datasets)

    spy_get_datasets.assert_called_with(None, 0)


@pytest.mark.asyncio
async def test_legacy_datasets_collection_is_returned_when_no_arguments_are_given():
    """This is for legacy compatibility"""
    stub_get_datasets = Mock(get_datasets)
    stub_get_datasets.return_value = [ORMDataset()]

    result = await paginate_datasets(crud_impl=stub_get_datasets)

    assert isinstance(result, list)
    assert isinstance(result[0], ORMDataset)
