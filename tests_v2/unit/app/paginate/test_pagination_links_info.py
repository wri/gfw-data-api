from unittest.mock import Mock

import pytest

from app.crud.datasets import count_datasets, get_datasets
from app.paginate.paginate import PaginationLinks, paginate_datasets


@pytest.mark.asyncio
async def test_links_are_returned():
    dummy_get_datasets = Mock(get_datasets)
    dummy_count_datasets = Mock(count_datasets)

    _, links, _ = await paginate_datasets(
        crud_impl=dummy_get_datasets, datasets_count_impl=dummy_count_datasets, page=1
    )

    assert isinstance(links, PaginationLinks)


@pytest.mark.asyncio
async def test_links_has_a_self_entry():
    dummy_get_datasets = Mock(get_datasets)
    dummy_count_datasets = Mock(count_datasets)

    _, links, _ = await paginate_datasets(
        crud_impl=dummy_get_datasets,
        datasets_count_impl=dummy_count_datasets,
        request_url="http://localhost:8008/datasets",
        page=2,
        size=10,
    )

    assert links.self == "http://localhost:8008/datasets?page[number]=2&page[size]=10"


@pytest.mark.asyncio
async def test_links_has_a_first_entry():
    dummy_get_datasets = Mock(get_datasets)
    dummy_count_datasets = Mock(count_datasets)

    _, links, _ = await paginate_datasets(
        crud_impl=dummy_get_datasets,
        datasets_count_impl=dummy_count_datasets,
        request_url="http://localhost:8008/datasets",
        page=2,
        size=10,
    )

    assert links.first == "http://localhost:8008/datasets?page[number]=1&page[size]=10"


@pytest.mark.asyncio
async def test_links_has_a_last_entry():
    dummy_get_datasets = Mock(get_datasets)
    stub_count_datasets = Mock(count_datasets)
    stub_count_datasets.return_value = 95

    _, links, _ = await paginate_datasets(
        crud_impl=dummy_get_datasets,
        datasets_count_impl=stub_count_datasets,
        request_url="http://localhost:8008/datasets",
        page=1,
        size=10,
    )

    assert links.last == "http://localhost:8008/datasets?page[number]=10&page[size]=10"


@pytest.mark.asyncio
async def test_links_has_a_prev_entry_when_not_on_the_first_page():
    dummy_get_datasets = Mock(get_datasets)
    dummy_count_datasets = Mock(count_datasets)

    _, links, _ = await paginate_datasets(
        crud_impl=dummy_get_datasets,
        datasets_count_impl=dummy_count_datasets,
        request_url="http://localhost:8008/datasets",
        page=3,
        size=10,
    )

    assert links.prev == "http://localhost:8008/datasets?page[number]=2&page[size]=10"


@pytest.mark.asyncio
async def test_links_has_a_next_entry_when_not_on_the_last_page():
    dummy_get_datasets = Mock(get_datasets)
    stub_count_datasets = Mock(count_datasets)
    stub_count_datasets.return_value = 95

    _, links, _ = await paginate_datasets(
        crud_impl=dummy_get_datasets,
        datasets_count_impl=stub_count_datasets,
        request_url="http://localhost:8008/datasets",
        page=3,
        size=10,
    )

    assert links.next == "http://localhost:8008/datasets?page[number]=4&page[size]=10"
