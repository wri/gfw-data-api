from typing import List
from unittest.mock import Mock

from app.crud.datasets import get_datasets
from app.models.orm.datasets import Dataset as ORMDataset


def paginate_datasets(crud_impl=get_datasets, size: int = None, page: int = 0) -> List[ORMDataset]:
    crud_impl(size, page)
    return [ORMDataset()]


def test_that_no_pagination_happens_for_default_values():
    """This is for legacy compatibility"""
    spy_get_datasets = Mock(get_datasets)

    paginate_datasets(spy_get_datasets)

    spy_get_datasets.assert_called_with(None, 0)


def test_that_legacy_datasets_collection_is_returned():
    result = paginate_datasets()

    assert isinstance(result, list)
    assert isinstance(result[0], ORMDataset)
