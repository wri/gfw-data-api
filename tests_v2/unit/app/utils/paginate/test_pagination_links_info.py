from unittest.mock import AsyncMock

import pytest

from app.models.pydantic.responses import PaginationLinks
from app.utils.paginate import paginate_collection

DONT_CARE: int = 1


@pytest.mark.asyncio
async def test_links_are_returned():
    dummy_get_collection = AsyncMock()
    dummy_count_collection = AsyncMock(return_value=DONT_CARE)

    _, links, _ = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=dummy_count_collection,
        page=1,
    )

    assert isinstance(links, PaginationLinks)


@pytest.mark.asyncio
async def test_links_has_a_self_entry():
    dummy_get_collection = AsyncMock()
    stub_count_collection = AsyncMock(return_value=15)

    _, links, _ = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=stub_count_collection,
        request_url="http://localhost:8008/datasets",
        page=2,
        size=10,
    )

    assert links.self == "http://localhost:8008/datasets?page[number]=2&page[size]=10"


@pytest.mark.asyncio
async def test_links_has_a_first_entry():
    dummy_get_collection = AsyncMock()
    stub_count_collection = AsyncMock(return_value=15)

    _, links, _ = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=stub_count_collection,
        request_url="http://localhost:8008/datasets",
        page=2,
        size=10,
    )

    assert links.first == "http://localhost:8008/datasets?page[number]=1&page[size]=10"


@pytest.mark.asyncio
async def test_links_has_a_last_entry():
    dummy_get_collection = AsyncMock()
    stub_count_collection = AsyncMock(return_value=95)

    _, links, _ = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=stub_count_collection,
        request_url="http://localhost:8008/datasets",
        page=1,
        size=10,
    )

    assert links.last == "http://localhost:8008/datasets?page[number]=10&page[size]=10"


@pytest.mark.asyncio
async def test_links_has_a_prev_entry_when_not_on_the_first_page():
    dummy_get_collection = AsyncMock()
    stub_count_collection = AsyncMock(return_value=25)

    _, links, _ = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=stub_count_collection,
        request_url="http://localhost:8008/datasets",
        page=3,
        size=10,
    )

    assert links.prev == "http://localhost:8008/datasets?page[number]=2&page[size]=10"


@pytest.mark.asyncio
async def test_links_has_an_empty_prev_entry_when_on_the_first_page():
    dummy_get_collection = AsyncMock()
    stub_count_collection = AsyncMock(return_value=95)

    _, links, _ = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=stub_count_collection,
        request_url="http://localhost:8008/datasets",
        page=1,
        size=10,
    )

    assert links.prev == ""


@pytest.mark.asyncio
async def test_raises_a_value_error_when_page_and_size_are_greater_than_total_pages():
    dummy_get_collection = AsyncMock()
    stub_count_collection = AsyncMock()
    stub_count_collection.return_value = 5

    with pytest.raises(ValueError):
        await paginate_collection(
            paged_items_fn=dummy_get_collection,
            item_count_fn=stub_count_collection,
            request_url="http://localhost:8008/datasets",
            page=2,
            size=10,
        )


@pytest.mark.asyncio
async def test_links_has_a_next_entry_when_not_on_the_last_page():
    dummy_get_collection = AsyncMock()
    stub_count_collection = AsyncMock(return_value=95)

    _, links, _ = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=stub_count_collection,
        request_url="http://localhost:8008/datasets",
        page=3,
        size=10,
    )

    assert links.next == "http://localhost:8008/datasets?page[number]=4&page[size]=10"


@pytest.mark.asyncio
async def test_links_has_an_empty_next_entry_when_on_the_last_page():
    dummy_get_collection = AsyncMock()
    stub_count_collection = AsyncMock(return_value=95)

    _, links, _ = await paginate_collection(
        paged_items_fn=dummy_get_collection,
        item_count_fn=stub_count_collection,
        request_url="http://localhost:8008/datasets",
        page=10,
        size=10,
    )

    assert links.next == ""
