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
