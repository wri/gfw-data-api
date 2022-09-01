from unittest.mock import AsyncMock

import pytest

from app.models.pydantic.responses import PaginationMeta
from app.utils.paginate import paginate_collection

DONT_CARE: int = 1


@pytest.mark.asyncio
async def test_sending_page_number_returns_a_collection_with_a_meta_section():
    dummy_get_collection = AsyncMock()
    dummy_count_collection = AsyncMock(return_value=DONT_CARE)

    _, _, meta = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=dummy_count_collection,
        page=1,
    )

    assert isinstance(meta, PaginationMeta)


@pytest.mark.asyncio
async def test_sending_size_number_returns_a_collection_with_a_meta_section():
    dummy_get_collection = AsyncMock()
    dummy_count_collection = AsyncMock(return_value=DONT_CARE)

    _, _, meta = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=dummy_count_collection,
        size=10,
    )

    assert isinstance(meta, PaginationMeta)


@pytest.mark.asyncio
async def test_pagination_meta_size_is_populated():
    dummy_get_collection = AsyncMock()
    dummy_count_collection = AsyncMock(return_value=DONT_CARE)

    _, _, meta = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=dummy_count_collection,
        page=1,
        size=10,
    )

    assert meta.size == 10


@pytest.mark.asyncio
async def test_pagination_meta_size_defaults_to_10_when_size_is_none():
    dummy_get_collection = AsyncMock()
    dummy_count_collection = AsyncMock(return_value=DONT_CARE)

    _, _, meta = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=dummy_count_collection,
        page=1,
        size=None,
    )

    assert meta.size == 10


@pytest.mark.asyncio
async def test_pagination_gets_total_row_count():
    dummy_get_collection = AsyncMock()
    spy_count_collection = AsyncMock(return_value=DONT_CARE)

    await paginate_collection(
        paged_items_fn=dummy_get_collection, item_count_fn=spy_count_collection, size=10
    )

    spy_count_collection.assert_called()


@pytest.mark.asyncio
async def test_pagination_meta_total_items_is_populated():
    dummy_get_collection = AsyncMock()
    stub_count_collection = AsyncMock(return_value=100)

    _, _, meta = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=stub_count_collection,
        size=10,
    )

    assert meta.total_items == 100


@pytest.mark.asyncio
async def test_pagination_meta_total_pages_is_populated():
    dummy_get_collection = AsyncMock()
    stub_count_collection = AsyncMock(return_value=100)

    _, _, meta = await paginate_collection(
        paged_items_fn=dummy_get_collection, item_count_fn=stub_count_collection, size=5
    )

    assert meta.total_pages == 20  # number_of_datasets / page_size


@pytest.mark.asyncio
async def test_pagination_meta_total_pages_is_1_when_there_are_0_items_in_a_collection():
    dummy_get_collection = AsyncMock()
    stub_count_collection = AsyncMock(return_value=0)

    _, _, meta = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=stub_count_collection,
        size=11,
    )

    assert meta.total_pages == 1
