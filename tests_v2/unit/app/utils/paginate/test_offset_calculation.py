from unittest.mock import AsyncMock

import pytest

from app.utils.paginate import paginate_collection

DONT_CARE: int = 1


@pytest.mark.asyncio
async def test_offset_is_0_for_page_1_when_size_is_given():
    spy_get_collection = AsyncMock()
    dummy_count_collection = AsyncMock(return_value=DONT_CARE)

    await paginate_collection(
        paged_items_fn=spy_get_collection,
        item_count_fn=dummy_count_collection,
        size=10,
        page=1,
    )

    spy_get_collection.assert_called_with(10, 0)


@pytest.mark.asyncio
async def test_offset_is_0_when_no_page_is_given():
    spy_get_collection = AsyncMock()
    dummy_count_collection = AsyncMock(return_value=DONT_CARE)

    await paginate_collection(
        paged_items_fn=spy_get_collection, item_count_fn=dummy_count_collection, size=10
    )

    spy_get_collection.assert_called_with(10, 0)


@pytest.mark.asyncio
async def test_offset_is_10_for_page_2_when_page_size_is_10():
    spy_get_collection = AsyncMock()
    stub_count_collection = AsyncMock(return_value=15)

    await paginate_collection(
        paged_items_fn=spy_get_collection,
        item_count_fn=stub_count_collection,
        size=10,
        page=2,
    )

    spy_get_collection.assert_called_with(10, 10)
