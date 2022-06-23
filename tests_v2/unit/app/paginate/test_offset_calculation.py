from unittest.mock import Mock

import pytest

from app.crud.datasets import count_datasets, get_datasets
from app.paginate.paginate import paginate_datasets

DONT_CARE: int = 1


@pytest.mark.asyncio
async def test_offset_is_0_for_page_1_when_size_is_given():
    spy_get_datasets = Mock(get_datasets)
    dummy_count_datasets = Mock(spec=count_datasets, return_value=DONT_CARE)

    await paginate_datasets(
        paged_items_fn=spy_get_datasets,
        item_count_fn=dummy_count_datasets,
        size=10,
        page=1,
    )

    spy_get_datasets.assert_called_with(size=10, offset=0)


@pytest.mark.asyncio
async def test_offset_is_0_when_no_page_is_given():
    spy_get_datasets = Mock(get_datasets)
    dummy_count_datasets = Mock(spec=count_datasets, return_value=DONT_CARE)

    await paginate_datasets(
        paged_items_fn=spy_get_datasets, item_count_fn=dummy_count_datasets, size=10
    )

    spy_get_datasets.assert_called_with(size=10, offset=0)


@pytest.mark.asyncio
async def test_offset_is_10_for_page_2_when_page_size_is_10():
    spy_get_datasets = Mock(get_datasets)
    stub_count_datasets = Mock(spec=count_datasets, return_value=15)

    await paginate_datasets(
        paged_items_fn=spy_get_datasets,
        item_count_fn=stub_count_datasets,
        size=10,
        page=2,
    )

    spy_get_datasets.assert_called_with(size=10, offset=10)
