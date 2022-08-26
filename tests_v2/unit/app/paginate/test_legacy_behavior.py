from unittest.mock import AsyncMock, Mock

import pytest

from app.models.orm.base import Base as ORMBase
from app.paginate.paginate import paginate_collection


@pytest.mark.asyncio
async def test_legacy_no_pagination_happens_for_default_values():
    """This is for legacy compatibility."""
    spy_get_orm_collection = AsyncMock()
    stub_item_count_fn = Mock()

    await paginate_collection(
        paged_items_fn=spy_get_orm_collection, item_count_fn=stub_item_count_fn
    )

    spy_get_orm_collection.assert_called_with(None, 0)


@pytest.mark.asyncio
async def test_legacy_collection_is_returned_when_no_arguments_are_given():
    """This is for legacy compatibility."""
    stub_get_collection = AsyncMock()
    stub_item_count_fn = Mock()
    stub_get_collection.return_value = [ORMBase()]

    data, _, _ = await paginate_collection(
        paged_items_fn=stub_get_collection, item_count_fn=stub_item_count_fn
    )

    assert isinstance(data, list)
    assert isinstance(data[0], ORMBase)


@pytest.mark.asyncio
async def test_legacy_meta_section_is_none_when_no_arguments_are_given():
    dummy_get_collection = AsyncMock()
    dummy_item_count_fn = Mock()

    _, _, meta = await paginate_collection(
        paged_items_fn=dummy_get_collection, item_count_fn=dummy_item_count_fn
    )

    assert meta is None
