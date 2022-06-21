from unittest.mock import Mock

import pytest

from app.crud.datasets import count_datasets, get_datasets
from app.paginate.paginate import PaginationMeta, paginate_datasets


@pytest.mark.asyncio
async def test_sending_page_number_returns_a_dataset_collection_with_a_meta_section():
    dummy_get_datasets = Mock(get_datasets)
    dummy_count_datasets = Mock(count_datasets)

    _, meta = await paginate_datasets(
        crud_impl=dummy_get_datasets, datasets_count_impl=dummy_count_datasets, page=1
    )

    assert isinstance(meta, PaginationMeta)


@pytest.mark.asyncio
async def test_sending_size_number_returns_a_dataset_collection_with_a_meta_section():
    dummy_get_datasets = Mock(get_datasets)
    dummy_count_datasets = Mock(count_datasets)

    _, meta = await paginate_datasets(
        crud_impl=dummy_get_datasets, datasets_count_impl=dummy_count_datasets, size=10
    )

    assert isinstance(meta, PaginationMeta)


@pytest.mark.asyncio
async def test_pagination_meta_size_is_populated():
    dummy_get_datasets = Mock(get_datasets)
    dummy_count_datasets = Mock(count_datasets)

    _, meta = await paginate_datasets(
        crud_impl=dummy_get_datasets,
        datasets_count_impl=dummy_count_datasets,
        page=1,
        size=10,
    )

    assert meta.size == 10


@pytest.mark.asyncio
async def test_pagination_gets_total_row_count():
    dummy_get_datasets = Mock(get_datasets)
    spy_count_datasets = Mock(count_datasets)

    await paginate_datasets(
        crud_impl=dummy_get_datasets, datasets_count_impl=spy_count_datasets, size=10
    )

    spy_count_datasets.assert_called()


@pytest.mark.asyncio
async def test_pagination_meta_total_items_is_populated():
    dummy_get_datasets = Mock(get_datasets)
    stub_count_datasets = Mock(count_datasets)
    stub_count_datasets.return_value = 100

    _, meta = await paginate_datasets(
        crud_impl=dummy_get_datasets, datasets_count_impl=stub_count_datasets, size=10
    )

    assert meta.total_items == 100


@pytest.mark.asyncio
async def test_pagination_meta_total_pages_is_populated():
    dummy_get_datasets = Mock(get_datasets)
    stub_count_datasets = Mock(count_datasets)
    stub_count_datasets.return_value = 100

    _, meta = await paginate_datasets(
        crud_impl=dummy_get_datasets, datasets_count_impl=stub_count_datasets, size=5
    )

    assert meta.total_pages == 20  # number_of_datasets / page_size


@pytest.mark.asyncio
async def test_pagination_meta_total_pages_adds_a_page_for_remainder_datasets():
    dummy_get_datasets = Mock(get_datasets)
    stub_count_datasets = Mock(count_datasets)
    stub_count_datasets.return_value = 100

    _, meta = await paginate_datasets(
        crud_impl=dummy_get_datasets, datasets_count_impl=stub_count_datasets, size=11
    )

    assert meta.total_pages == 10  # number_of_datasets / page_size
