from unittest.mock import Mock

import pytest

from app.crud.datasets import count_datasets, get_datasets
from app.paginate.paginate import paginate_datasets


@pytest.mark.asyncio
async def test_offset_is_0_for_page_1_when_size_is_given():
    spy_get_datasets = Mock(get_datasets)
    dummy_count_datasets = Mock(count_datasets)

    await paginate_datasets(
        crud_impl=spy_get_datasets,
        datasets_count_impl=dummy_count_datasets,
        size=10,
        page=1,
    )

    spy_get_datasets.assert_called_with(size=10, offset=0)


@pytest.mark.asyncio
async def test_offset_is_0_when_no_page_is_given():
    spy_get_datasets = Mock(get_datasets)
    dummy_count_datasets = Mock(count_datasets)

    await paginate_datasets(
        crud_impl=spy_get_datasets, datasets_count_impl=dummy_count_datasets, size=10
    )

    spy_get_datasets.assert_called_with(size=10, offset=0)


@pytest.mark.asyncio
async def test_offset_is_10_for_page_2_when_page_size_is_10():
    spy_get_datasets = Mock(get_datasets)
    dummy_count_datasets = Mock(count_datasets)

    await paginate_datasets(
        crud_impl=spy_get_datasets,
        datasets_count_impl=dummy_count_datasets,
        size=10,
        page=2,
    )

    spy_get_datasets.assert_called_with(size=10, offset=10)
