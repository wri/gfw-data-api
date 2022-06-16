from unittest.mock import Mock

import pytest

from app.crud.datasets import get_datasets
from app.models.orm.datasets import Dataset as ORMDataset
from app.paginate.paginate import paginate_datasets


@pytest.mark.asyncio
async def test_legacy_no_pagination_happens_for_default_values():
    """This is for legacy compatibility."""
    spy_get_datasets = Mock(get_datasets)

    await paginate_datasets(crud_impl=spy_get_datasets)

    spy_get_datasets.assert_called_with(None, 0)


@pytest.mark.asyncio
async def test_legacy_datasets_collection_is_returned_when_no_arguments_are_given():
    """This is for legacy compatibility."""
    stub_get_datasets = Mock(get_datasets)
    stub_get_datasets.return_value = [ORMDataset()]

    data, _ = await paginate_datasets(crud_impl=stub_get_datasets)

    assert isinstance(data, list)
    assert isinstance(data[0], ORMDataset)


@pytest.mark.asyncio
async def test_legacy_meta_section_is_none_when_no_arguments_are_given():
    dummy_get_datasets = Mock(get_datasets)

    _, meta = await paginate_datasets(crud_impl=dummy_get_datasets)

    assert meta is None


@pytest.mark.asyncio
async def test_sending_page_number_returns_a_dataset_collection_with_a_meta_section():
    dummy_get_datasets = Mock(get_datasets)

    _, meta = await paginate_datasets(crud_impl=dummy_get_datasets, page=1)

    assert isinstance(meta, dict)
