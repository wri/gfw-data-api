from math import ceil
from typing import List, Optional, Tuple

from app.crud.datasets import count_datasets, get_datasets
from app.models.orm.datasets import Dataset as ORMDataset


class PaginationMeta:
    size: int
    total_items: int
    total_pages: int

    def __init__(self, size: int, total_items: int):
        self.size = size
        self.total_items = total_items
        self.total_pages = ceil(total_items / size)


async def paginate_datasets(
    crud_impl=get_datasets,
    datasets_count_impl=count_datasets,
    size: Optional[int] = None,
    page: Optional[int] = 0,
) -> Tuple[List[ORMDataset], Optional[PaginationMeta]]:
    data = await crud_impl(size, _calculate_offset(page, size))

    if size is None and page == 0:
        return data, None

    total_datasets = await datasets_count_impl()
    meta = PaginationMeta(size=size or 1, total_items=total_datasets)

    return data, meta


def _calculate_offset(page, size):
    return (size or 1) * max(0, page - 1)
